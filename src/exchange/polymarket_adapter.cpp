#include "market_data/exchange/polymarket_adapter.hpp"

#include <algorithm>
#include <deque>
#include <functional>
#include <optional>
#include <random>
#include <thread>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <nlohmann/json.hpp>

#include "market_data/exchange/polymarket_parser.hpp"
#include "market_data/logging.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = asio::ip::tcp;

// Gamma caps a single page; anything larger is silently truncated.
constexpr std::size_t kGammaPageSize = 100;
// Refuse to page forever if the venue ignores our offset.
constexpr std::size_t kMaxDiscoveryPages = 200;

// Splits "wss://host[:port]/path" into its parts.
bool parse_ws_url(const std::string& url,
                  std::string& host,
                  std::string& port,
                  std::string& target) {
    constexpr std::string_view kWss = "wss://";
    if (url.rfind(kWss, 0) != 0) {
        return false;
    }
    std::string rest = url.substr(kWss.size());

    const auto slash = rest.find('/');
    if (slash == std::string::npos) {
        target = "/";
    } else {
        target = rest.substr(slash);
        rest = rest.substr(0, slash);
    }
    if (rest.empty()) {
        return false;
    }

    if (const auto colon = rest.rfind(':'); colon != std::string::npos) {
        host = rest.substr(0, colon);
        port = rest.substr(colon + 1);
    } else {
        host = rest;
        port = "443";
    }
    return !host.empty() && !port.empty();
}

}  // namespace

// ---------------------------------------------------------------------------
// Session: the websocket half of the adapter.
//
// Everything runs on a single strand over one io_context thread, so the socket,
// the outbox, the subscription set and the backoff state need no locking among
// themselves; only the fields touched by other threads (the subscription list
// set from the engine, the connected flag read by status) are guarded.
// ---------------------------------------------------------------------------
class PolymarketAdapter::Session : public std::enable_shared_from_this<PolymarketAdapter::Session> {
public:
    using FrameHandler = std::function<void(std::string_view, std::size_t)>;
    using StateHandler = std::function<void(ConnectionState, std::string, std::uint32_t, std::size_t)>;

    Session(Config config, FrameHandler on_frame, StateHandler on_state)
        : config_(std::move(config)),
          on_frame_(std::move(on_frame)),
          on_state_(std::move(on_state)),
          strand_(asio::make_strand(ioc_)),
          resolver_(strand_),
          reconnect_timer_(strand_),
          ping_timer_(strand_),
          read_deadline_(strand_),
          rng_(std::random_device{}()) {
        ssl_ctx_.set_default_verify_paths();
        ssl_ctx_.set_verify_mode(asio::ssl::verify_peer);
    }

    ~Session() { stop(); }

    void start() {
        if (running_.exchange(true)) {
            return;
        }
        // The work guard keeps run() alive across the gaps between a
        // disconnect and the next reconnect attempt.
        work_guard_.emplace(asio::make_work_guard(ioc_));
        thread_ = std::thread([self = shared_from_this()] {
            try {
                self->ioc_.run();
            } catch (const std::exception& e) {
                MDE_ERROR("websocket_io_failed", field("error", e.what()));
            }
        });
        asio::post(strand_, [self = shared_from_this()] { self->open(); });
    }

    void stop() {
        if (!running_.exchange(false)) {
            return;
        }
        asio::post(strand_, [self = shared_from_this()] { self->close_socket("shutdown"); });
        work_guard_.reset();
        if (thread_.joinable()) {
            thread_.join();
        }
        connected_.store(false, std::memory_order_relaxed);
    }

    // Callable from any thread.
    void set_subscription(std::vector<TokenId> tokens) {
        asio::post(strand_, [self = shared_from_this(), tokens = std::move(tokens)]() mutable {
            self->tokens_ = std::move(tokens);
            if (self->connected_.load(std::memory_order_relaxed)) {
                self->send_subscription();
            }
        });
    }

    [[nodiscard]] bool connected() const { return connected_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::uint64_t reconnect_count() const {
        return reconnects_.load(std::memory_order_relaxed);
    }

private:
    using Stream = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;

    // --- connection lifecycle ---------------------------------------------

    void open() {
        if (!running_.load(std::memory_order_relaxed)) {
            return;
        }

        std::string host;
        std::string port;
        std::string target;
        if (!parse_ws_url(config_.polymarket.market_ws_url, host, port, target)) {
            MDE_ERROR("websocket_bad_url", field("url", config_.polymarket.market_ws_url));
            report(ConnectionState::Failed, "unparseable websocket url");
            running_.store(false);
            work_guard_.reset();
            return;
        }
        host_ = host;
        target_ = target;

        report(ConnectionState::Connecting, config_.polymarket.market_ws_url);
        stream_ = std::make_unique<Stream>(strand_, ssl_ctx_);

        // Beast's own keep-alive pings are WS protocol pings; this venue wants
        // an application-level text "PING", so we drive the heartbeat ourselves
        // and disable Beast's idle machinery to avoid two competing timers.
        stream_->set_option(websocket::stream_base::timeout{
            std::chrono::milliseconds{config_.health.read_timeout_ms},  // handshake
            websocket::stream_base::none(),                             // idle
            false});                                                    // keep_alive_pings
        stream_->set_option(websocket::stream_base::decorator([](websocket::request_type& req) {
            req.set(beast::http::field::user_agent, "market-data-engine/0.1");
        }));

        resolver_.async_resolve(
            host, port,
            asio::bind_executor(strand_, [self = shared_from_this()](beast::error_code ec,
                                                                     tcp::resolver::results_type r) {
                self->on_resolve(ec, std::move(r));
            }));
    }

    void on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
        if (ec) {
            return fail(ec, "resolve");
        }
        beast::get_lowest_layer(*stream_).expires_after(
            std::chrono::milliseconds{config_.health.read_timeout_ms});
        beast::get_lowest_layer(*stream_).async_connect(
            results, asio::bind_executor(strand_, [self = shared_from_this()](
                                                      beast::error_code e,
                                                      const tcp::resolver::results_type::endpoint_type&) {
                self->on_connect(e);
            }));
    }

    void on_connect(beast::error_code ec) {
        if (ec) {
            return fail(ec, "tcp_connect");
        }
        // Without SNI the CDN serves a certificate for the wrong host and
        // verification fails with a confusing error.
        if (SSL_set_tlsext_host_name(stream_->next_layer().native_handle(), host_.c_str()) == 0) {
            return fail(beast::error_code{static_cast<int>(::ERR_get_error()),
                                          asio::error::get_ssl_category()},
                        "sni");
        }
        stream_->next_layer().set_verify_callback(asio::ssl::host_name_verification(host_));

        stream_->next_layer().async_handshake(
            asio::ssl::stream_base::client,
            asio::bind_executor(strand_, [self = shared_from_this()](beast::error_code e) {
                self->on_tls_handshake(e);
            }));
    }

    void on_tls_handshake(beast::error_code ec) {
        if (ec) {
            return fail(ec, "tls_handshake");
        }
        // Beast's websocket layer manages its own timeouts from here on.
        beast::get_lowest_layer(*stream_).expires_never();
        stream_->async_handshake(
            host_, target_,
            asio::bind_executor(strand_, [self = shared_from_this()](beast::error_code e) {
                self->on_ws_handshake(e);
            }));
    }

    void on_ws_handshake(beast::error_code ec) {
        if (ec) {
            return fail(ec, "ws_handshake");
        }

        connected_.store(true, std::memory_order_relaxed);
        backoff_ms_ = config_.reconnect.initial_backoff_ms;
        attempt_ = 0;
        report(ConnectionState::Connected, config_.polymarket.market_ws_url);

        send_subscription();
        arm_ping();
        arm_read_deadline();
        read();
    }

    // --- subscription ------------------------------------------------------

    void send_subscription() {
        if (tokens_.empty()) {
            MDE_WARN("subscription_skipped", field("reason", "no tokens selected"));
            return;
        }
        const nlohmann::json message{{"assets_ids", tokens_}, {"type", "market"}};
        enqueue(message.dump());
        report(ConnectionState::SubscriptionSent, "", tokens_.size());
        MDE_INFO("subscription_sent", field("tokens", tokens_.size()));
    }

    // --- reads -------------------------------------------------------------

    void read() {
        if (!running_.load(std::memory_order_relaxed) || !stream_) {
            return;
        }
        stream_->async_read(
            buffer_, asio::bind_executor(strand_, [self = shared_from_this()](
                                                      beast::error_code ec, std::size_t bytes) {
                self->on_read(ec, bytes);
            }));
    }

    void on_read(beast::error_code ec, std::size_t bytes) {
        if (ec) {
            return fail(ec, "read");
        }

        // Any frame proves the peer is alive, so the deadline resets on data
        // as well as on the heartbeat reply.
        arm_read_deadline();

        const auto data = buffer_.data();
        const std::string_view frame{static_cast<const char*>(data.data()), data.size()};
        on_frame_(frame, bytes);
        buffer_.consume(buffer_.size());

        read();
    }

    // --- writes ------------------------------------------------------------

    // Beast permits only one outstanding write, so everything funnels through
    // an outbox drained on the strand.
    void enqueue(std::string message) {
        outbox_.push_back(std::move(message));
        if (!writing_) {
            write_next();
        }
    }

    void write_next() {
        if (outbox_.empty() || !stream_) {
            writing_ = false;
            return;
        }
        writing_ = true;
        stream_->text(true);
        stream_->async_write(
            asio::buffer(outbox_.front()),
            asio::bind_executor(strand_, [self = shared_from_this()](beast::error_code ec,
                                                                     std::size_t) {
                if (ec) {
                    self->writing_ = false;
                    return self->fail(ec, "write");
                }
                self->outbox_.pop_front();
                self->write_next();
            }));
    }

    // --- timers ------------------------------------------------------------

    void arm_ping() {
        ping_timer_.expires_after(std::chrono::milliseconds{config_.health.ping_interval_ms});
        ping_timer_.async_wait(
            asio::bind_executor(strand_, [self = shared_from_this()](beast::error_code ec) {
                if (ec || !self->connected_.load(std::memory_order_relaxed)) {
                    return;
                }
                // The venue's application-level heartbeat: a bare text "PING",
                // answered with a bare text "PONG".
                self->enqueue("PING");
                self->arm_ping();
            }));
    }

    void arm_read_deadline() {
        read_deadline_.expires_after(std::chrono::milliseconds{config_.health.read_timeout_ms});
        read_deadline_.async_wait(
            asio::bind_executor(strand_, [self = shared_from_this()](beast::error_code ec) {
                if (ec == asio::error::operation_aborted) {
                    return;  // Rearmed by a fresh frame.
                }
                if (!self->connected_.load(std::memory_order_relaxed)) {
                    return;
                }
                // A silent socket that TCP has not yet declared dead. Tear it
                // down ourselves rather than sit on a stale feed.
                MDE_WARN("feed_silent", field("timeout_ms", self->config_.health.read_timeout_ms));
                self->fail(beast::error_code{asio::error::timed_out}, "read_timeout");
            }));
    }

    // --- failure and reconnect ---------------------------------------------

    void fail(beast::error_code ec, const char* stage) {
        if (!running_.load(std::memory_order_relaxed)) {
            return;
        }
        // Only the first failure of a connection attempt matters; the rest are
        // the fallout of tearing the socket down.
        if (failing_) {
            return;
        }
        failing_ = true;

        const bool was_connected = connected_.exchange(false, std::memory_order_relaxed);
        MDE_WARN("connection_lost", field("stage", stage), field("error", ec.message()),
                 field("was_connected", was_connected));
        report(ConnectionState::Disconnected, std::string{stage} + ": " + ec.message());

        close_socket(stage);
        schedule_reconnect();
    }

    void close_socket(const char* reason) {
        static_cast<void>(reason);
        connected_.store(false, std::memory_order_relaxed);
        ping_timer_.cancel();
        read_deadline_.cancel();
        reconnect_timer_.cancel();
        outbox_.clear();
        writing_ = false;
        buffer_.consume(buffer_.size());

        if (stream_) {
            beast::error_code ignored;
            // Best-effort TCP close: a graceful websocket close handshake can
            // block on a peer that has already gone away.
            beast::get_lowest_layer(*stream_).socket().shutdown(tcp::socket::shutdown_both, ignored);
            beast::get_lowest_layer(*stream_).socket().close(ignored);
            stream_.reset();
        }
    }

    void schedule_reconnect() {
        if (!running_.load(std::memory_order_relaxed)) {
            return;
        }
        ++attempt_;
        if (config_.reconnect.max_attempts != 0 && attempt_ > config_.reconnect.max_attempts) {
            MDE_ERROR("reconnect_giving_up", field("attempts", attempt_));
            report(ConnectionState::Failed, "exhausted reconnect attempts", 0, attempt_);
            running_.store(false);
            work_guard_.reset();
            return;
        }

        // Full jitter on top of exponential backoff, so a fleet of collectors
        // does not stampede the venue in lockstep after an outage.
        std::uniform_real_distribution<double> jitter(0.0, config_.reconnect.jitter_fraction);
        const auto delay = static_cast<std::uint64_t>(
            static_cast<double>(backoff_ms_) * (1.0 + jitter(rng_)));

        MDE_INFO("reconnect_scheduled", field("attempt", attempt_),
                 field("delay_ms", static_cast<std::uint64_t>(delay)));
        report(ConnectionState::Reconnecting, "backoff", 0, attempt_);
        reconnects_.fetch_add(1, std::memory_order_relaxed);

        backoff_ms_ = static_cast<std::uint32_t>(std::min<double>(
            static_cast<double>(backoff_ms_) * config_.reconnect.backoff_multiplier,
            static_cast<double>(config_.reconnect.max_backoff_ms)));

        reconnect_timer_.expires_after(std::chrono::milliseconds{delay});
        reconnect_timer_.async_wait(
            asio::bind_executor(strand_, [self = shared_from_this()](beast::error_code ec) {
                if (ec) {
                    return;
                }
                self->failing_ = false;
                self->open();
            }));
    }

    void report(ConnectionState state,
                std::string detail = {},
                std::size_t tokens = 0,
                std::uint32_t attempt = 0) {
        if (on_state_) {
            on_state_(state, std::move(detail), attempt, tokens);
        }
    }

    Config config_;
    FrameHandler on_frame_;
    StateHandler on_state_;

    asio::io_context ioc_{1};
    asio::ssl::context ssl_ctx_{asio::ssl::context::tlsv12_client};
    asio::strand<asio::io_context::executor_type> strand_;
    std::optional<asio::executor_work_guard<asio::io_context::executor_type>> work_guard_;
    std::thread thread_;

    tcp::resolver resolver_;
    std::unique_ptr<Stream> stream_;
    beast::flat_buffer buffer_;

    asio::steady_timer reconnect_timer_;
    asio::steady_timer ping_timer_;
    asio::steady_timer read_deadline_;

    // Strand-confined state.
    std::string host_;
    std::string target_;
    std::vector<TokenId> tokens_;
    std::deque<std::string> outbox_;
    bool writing_ = false;
    bool failing_ = false;
    std::uint32_t backoff_ms_ = 500;
    std::uint32_t attempt_ = 0;
    std::mt19937 rng_;

    // Cross-thread state.
    std::atomic<bool> running_{false};
    std::atomic<bool> connected_{false};
    std::atomic<std::uint64_t> reconnects_{0};
};

// ---------------------------------------------------------------------------
// PolymarketAdapter
// ---------------------------------------------------------------------------

PolymarketAdapter::PolymarketAdapter(Config config)
    : config_(std::move(config)),
      http_(std::chrono::seconds{20}),
      resync_http_(std::chrono::seconds{10}) {}

PolymarketAdapter::~PolymarketAdapter() { PolymarketAdapter::disconnect(); }

std::string PolymarketAdapter::name() const { return polymarket::kExchangeName; }

void PolymarketAdapter::set_event_sink(EventSink sink) {
    const std::lock_guard lock(sink_mutex_);
    sink_ = std::move(sink);
}

void PolymarketAdapter::emit(MarketEvent&& event) {
    EventSink sink;
    {
        const std::lock_guard lock(sink_mutex_);
        sink = sink_;
    }
    if (sink) {
        sink(std::move(event));
    }
}

std::vector<Market> PolymarketAdapter::discover_markets() {
    std::vector<Market> discovered;
    const auto fetched_at = now_exchange();

    // We only need enough markets to rank the top N by volume, but Gamma's
    // ordering is server-side, so a few pages give plenty of headroom without
    // sweeping the whole venue.
    const std::size_t wanted = config_.subscribe.top_markets_by_volume;
    const std::size_t page_budget =
        std::min<std::size_t>(kMaxDiscoveryPages, (wanted * 4) / kGammaPageSize + 2);

    for (std::size_t page = 0; page < page_budget; ++page) {
        const std::vector<HttpQueryParam> params{
            {"active", "true"},
            {"closed", "false"},
            {"archived", "false"},
            {"limit", std::to_string(kGammaPageSize)},
            {"offset", std::to_string(page * kGammaPageSize)},
            {"order", "volumeNum"},
            {"ascending", "false"},
        };

        const auto response = http_.get(config_.polymarket.gamma_base_url, "/markets", params);
        if (!response.ok()) {
            stats_.discovery_failures.fetch_add(1, std::memory_order_relaxed);
            MDE_WARN("discovery_request_failed", field("page", page),
                     field("status", response.status),
                     field("error", response.transport_error));
            break;
        }

        std::vector<std::string> errors;
        auto markets = polymarket::parse_gamma_markets(response.body, fetched_at, &errors);
        if (!errors.empty()) {
            // Individual bad markets are expected (unfunded, mid-deployment);
            // they are counted, not fatal.
            stats_.malformed_messages.fetch_add(errors.size(), std::memory_order_relaxed);
            MDE_DEBUG("discovery_entries_skipped", field("page", page),
                      field("skipped", errors.size()), field("first", errors.front()));
        }
        if (markets.empty()) {
            break;  // Ran out of pages.
        }

        const std::size_t before = discovered.size();
        for (auto& market : markets) {
            if (config_.subscribe.min_reported_liquidity > 0.0 &&
                market.reported_liquidity < config_.subscribe.min_reported_liquidity) {
                continue;
            }
            discovered.push_back(std::move(market));
        }
        static_cast<void>(before);

        if (markets.size() < kGammaPageSize) {
            break;  // Last page.
        }
    }

    MDE_INFO("markets_discovered", field("count", discovered.size()));
    return discovered;
}

bool PolymarketAdapter::request_snapshot(const TokenId& token) {
    stats_.snapshots_requested.fetch_add(1, std::memory_order_relaxed);

    const auto response =
        resync_http_.get(config_.polymarket.clob_base_url, "/book", {{"token_id", token}});
    if (!response.ok()) {
        stats_.snapshot_request_failures.fetch_add(1, std::memory_order_relaxed);
        MDE_WARN("snapshot_request_failed", field("token", token),
                 field("status", response.status), field("error", response.transport_error));
        return false;
    }

    BookSnapshotEvent event;
    std::string error;
    if (!polymarket::parse_clob_book(response.body, now_receive(), event, &error)) {
        stats_.snapshot_request_failures.fetch_add(1, std::memory_order_relaxed);
        MDE_WARN("snapshot_parse_failed", field("token", token), field("error", error));
        return false;
    }

    // The CLOB echoes the asset_id, but pin it to what we asked for so a venue
    // bug cannot reroute a snapshot into the wrong book.
    if (event.header.token_id != token) {
        stats_.snapshot_request_failures.fetch_add(1, std::memory_order_relaxed);
        MDE_WARN("snapshot_token_mismatch", field("requested", token),
                 field("returned", event.header.token_id));
        return false;
    }

    MDE_DEBUG("snapshot_received", field("token", token), field("bids", event.bids.size()),
              field("asks", event.asks.size()));
    emit(MarketEvent{std::move(event)});
    return true;
}

void PolymarketAdapter::connect() {
    if (session_) {
        return;
    }

    auto on_frame = [this](std::string_view frame, std::size_t bytes) {
        stats_.frames_received.fetch_add(1, std::memory_order_relaxed);
        stats_.bytes_received.fetch_add(bytes, std::memory_order_relaxed);

        auto outcome = polymarket::parse_ws_frame(frame, now_receive());
        if (outcome.was_control_frame) {
            return;
        }
        if (outcome.has_errors()) {
            stats_.malformed_messages.fetch_add(outcome.errors.size(), std::memory_order_relaxed);
            // Logged at debug: a venue that adds an event type would otherwise
            // flood the log at feed rate.
            MDE_DEBUG("malformed_message", field("count", outcome.errors.size()),
                      field("first", outcome.errors.front()));
        }
        for (auto& event : outcome.events) {
            emit(std::move(event));
        }
    };

    auto on_state = [this](ConnectionState state, std::string detail, std::uint32_t attempt,
                           std::size_t tokens) {
        if (state == ConnectionState::Reconnecting) {
            stats_.reconnects.fetch_add(1, std::memory_order_relaxed);
        }
        ConnectionEvent event;
        event.header.exchange = polymarket::kExchangeName;
        event.header.exchange_timestamp = now_exchange();
        event.header.receive_timestamp = now_receive();
        event.state = state;
        event.detail = std::move(detail);
        event.attempt = attempt;
        event.subscribed_tokens = tokens;
        emit(MarketEvent{std::move(event)});
    };

    session_ = std::make_shared<Session>(config_, std::move(on_frame), std::move(on_state));
    session_->start();
    if (!pending_tokens_.empty()) {
        session_->set_subscription(pending_tokens_);
    }
}

void PolymarketAdapter::subscribe(const std::vector<TokenId>& tokens) {
    std::vector<TokenId> selected = tokens;
    if (config_.subscribe.max_tokens != 0 && selected.size() > config_.subscribe.max_tokens) {
        MDE_WARN("subscription_truncated", field("requested", tokens.size()),
                 field("max_tokens", config_.subscribe.max_tokens));
        selected.resize(config_.subscribe.max_tokens);
    }
    pending_tokens_ = selected;
    if (session_) {
        session_->set_subscription(std::move(selected));
    }
}

void PolymarketAdapter::disconnect() {
    if (session_) {
        session_->stop();
        session_.reset();
    }
}

bool PolymarketAdapter::is_connected() const {
    return session_ && session_->connected();
}

AdapterMetricsView PolymarketAdapter::adapter_metrics() const {
    constexpr auto relaxed = std::memory_order_relaxed;
    AdapterMetricsView view;
    view.frames_received = stats_.frames_received.load(relaxed);
    view.bytes_received = stats_.bytes_received.load(relaxed);
    view.malformed_messages = stats_.malformed_messages.load(relaxed);
    view.reconnects = stats_.reconnects.load(relaxed);
    view.snapshots_requested = stats_.snapshots_requested.load(relaxed);
    view.snapshot_request_failures = stats_.snapshot_request_failures.load(relaxed);
    return view;
}

}  // namespace market_data

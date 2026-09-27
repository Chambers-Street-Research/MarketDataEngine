#include "market_data/http_client.hpp"

#include <cctype>
#include <iomanip>
#include <sstream>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/version.hpp>

namespace market_data {
namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

}  // namespace

struct HttpClient::Impl {
    std::chrono::milliseconds timeout;
    // One long-lived TLS context: loading the trust store on every request
    // would dominate the cost of a discovery sweep.
    mutable asio::ssl::context ssl_ctx{asio::ssl::context::tlsv12_client};

    explicit Impl(std::chrono::milliseconds t) : timeout(t) {
        ssl_ctx.set_default_verify_paths();
        ssl_ctx.set_verify_mode(asio::ssl::verify_peer);
    }
};

HttpClient::HttpClient(std::chrono::milliseconds timeout)
    : impl_(std::make_unique<Impl>(timeout)) {}

HttpClient::~HttpClient() = default;
HttpClient::HttpClient(HttpClient&&) noexcept = default;
HttpClient& HttpClient::operator=(HttpClient&&) noexcept = default;

std::string HttpClient::url_encode(const std::string& value) {
    std::ostringstream out;
    out << std::hex << std::uppercase << std::setfill('0');
    for (const char raw : value) {
        const auto c = static_cast<unsigned char>(raw);
        if (std::isalnum(c) != 0 || c == '-' || c == '_' || c == '.' || c == '~') {
            out << static_cast<char>(c);
        } else {
            out << '%' << std::setw(2) << static_cast<int>(c);
        }
    }
    return out.str();
}

bool HttpClient::parse_base_url(const std::string& url,
                                std::string& out_host,
                                std::string& out_port,
                                bool& out_tls) {
    constexpr std::string_view kHttps = "https://";
    constexpr std::string_view kHttp = "http://";

    std::string rest;
    if (url.rfind(kHttps, 0) == 0) {
        out_tls = true;
        rest = url.substr(kHttps.size());
    } else if (url.rfind(kHttp, 0) == 0) {
        out_tls = false;
        rest = url.substr(kHttp.size());
    } else {
        return false;
    }

    // Strip any path the caller left on the base URL.
    if (const auto slash = rest.find('/'); slash != std::string::npos) {
        rest = rest.substr(0, slash);
    }
    if (rest.empty()) {
        return false;
    }

    if (const auto colon = rest.rfind(':'); colon != std::string::npos) {
        out_host = rest.substr(0, colon);
        out_port = rest.substr(colon + 1);
    } else {
        out_host = rest;
        out_port = out_tls ? "443" : "80";
    }
    return !out_host.empty() && !out_port.empty();
}

HttpResponse HttpClient::get(const std::string& base_url,
                             const std::string& target,
                             const std::vector<HttpQueryParam>& params) const {
    HttpResponse result;

    std::string host;
    std::string port;
    bool tls = true;
    if (!parse_base_url(base_url, host, port, tls)) {
        result.transport_error = "unsupported base url: " + base_url;
        return result;
    }
    if (!tls) {
        // Plain HTTP is rejected rather than silently supported: every venue we
        // talk to is HTTPS, so an http:// URL in config is a mistake.
        result.transport_error = "plain http is not supported: " + base_url;
        return result;
    }

    std::string full_target = target;
    for (std::size_t i = 0; i < params.size(); ++i) {
        full_target += (i == 0 ? '?' : '&');
        full_target += url_encode(params[i].key);
        full_target += '=';
        full_target += url_encode(params[i].value);
    }

    try {
        asio::io_context ioc;
        tcp::resolver resolver(ioc);
        beast::ssl_stream<beast::tcp_stream> stream(ioc, impl_->ssl_ctx);

        // SNI: without it these CDNs return a certificate for the wrong host.
        if (SSL_set_tlsext_host_name(stream.native_handle(), host.c_str()) == 0) {
            result.transport_error = "failed to set TLS SNI hostname";
            return result;
        }
        stream.set_verify_callback(asio::ssl::host_name_verification(host));

        const auto endpoints = resolver.resolve(host, port);
        beast::get_lowest_layer(stream).expires_after(impl_->timeout);
        beast::get_lowest_layer(stream).connect(endpoints);
        stream.handshake(asio::ssl::stream_base::client);

        http::request<http::empty_body> req{http::verb::get, full_target, 11};
        req.set(http::field::host, host);
        req.set(http::field::user_agent, "market-data-engine/" BOOST_BEAST_VERSION_STRING);
        req.set(http::field::accept, "application/json");

        beast::get_lowest_layer(stream).expires_after(impl_->timeout);
        http::write(stream, req);

        beast::flat_buffer buffer;
        http::response<http::string_body> res;
        http::read(stream, buffer, res);

        result.status = static_cast<int>(res.result_int());
        result.body = std::move(res.body());

        // A truncated-stream shutdown is normal for servers that close without
        // a TLS close_notify; it is not a failure of a response we already read.
        beast::error_code ec;
        stream.shutdown(ec);
    } catch (const std::exception& e) {
        result.transport_error = e.what();
    }

    return result;
}

}  // namespace market_data

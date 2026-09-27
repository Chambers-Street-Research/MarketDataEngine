// Minimal synchronous HTTPS GET client built on Boost.Beast.
//
// Discovery and order-book resync are request/response operations that happen
// on their own thread, far off the hot path, so a synchronous client keeps the
// code honest and short. The live market-data feed is asynchronous; see
// websocket_client.hpp.
#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace market_data {

struct HttpResponse {
    int status = 0;
    std::string body;
    // Populated instead of a status when the request never completed.
    std::string transport_error;

    [[nodiscard]] bool ok() const { return transport_error.empty() && status >= 200 && status < 300; }
};

struct HttpQueryParam {
    std::string key;
    std::string value;
};

class HttpClient {
public:
    explicit HttpClient(std::chrono::milliseconds timeout = std::chrono::seconds{15});
    ~HttpClient();

    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;
    HttpClient(HttpClient&&) noexcept;
    HttpClient& operator=(HttpClient&&) noexcept;

    // `base_url` is "https://host[:port]"; `target` is the path, e.g. "/markets".
    // Never throws: transport failures come back in HttpResponse::transport_error.
    [[nodiscard]] HttpResponse get(const std::string& base_url,
                                   const std::string& target,
                                   const std::vector<HttpQueryParam>& params = {}) const;

    // Percent-encodes a query parameter value.
    [[nodiscard]] static std::string url_encode(const std::string& value);

    // Splits "https://host:port" into its parts. Returns false if `url` is not
    // a scheme we support.
    static bool parse_base_url(const std::string& url,
                               std::string& out_host,
                               std::string& out_port,
                               bool& out_tls);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace market_data

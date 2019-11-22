#include <raft/logger.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/throw_exception.hpp>

namespace raft {
    string LogLevel::ToString() const {
        switch (value) {
        case LogLevel::all:
            return "all";
        case LogLevel::debug:
            return "debug";
        case LogLevel::info:
            return "info";
        case LogLevel::warn:
            return "warn";
        case LogLevel::error:
            return "error";
        case LogLevel::fatal:
            return "fatal";
        case LogLevel::off:
        default:
            return "off";
        }
    }

    namespace detail {
        class LogContextImpl {
        public:
            LogLevel     level;
            string       file;
            uint64_t     line;
            string       method;
        };
    }

    LogContext::LogContext(
        LogLevel level,
        const char* file,
        uint64_t line,
        const char* method
    ) : impl(new detail::LogContextImpl) {
        impl->level = level;
        impl->file = boost::filesystem::path(file).generic_string();
        impl->line = line;
        impl->method = method;
    }

    LogContext::~LogContext() {
    }

    const string &LogContext::GetFile() const {
        return impl->file;
    }

    uint64_t LogContext::GetLineNumber() const {
        return impl->line;
    }

    const string &LogContext::GetMethod() const {
        return impl->method;
    }

    LogLevel LogContext::GetLogLevel() const {
        return impl->level;
    }

    string LogContext::ToString() const {
        return impl->file + ":" + boost::lexical_cast<string>(impl->line) + " " + impl->method;
    }

    void DefaultLogger::log(const LogContext &ctx, const string &msg) {
        if (ctx.GetLogLevel() >= logLevel) {
            std::cout
                << "[" << ctx.GetLogLevel().ToString() << " " << ctx.ToString() << "] "
                << msg << std::endl;
        }
        if (ctx.GetLogLevel() == LogLevel::fatal) {
            BOOST_THROW_EXCEPTION(std::runtime_error(ctx.ToString() + msg));
        }
    }
}

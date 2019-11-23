#pragma once
#include <boost/noncopyable.hpp>
#include <boost/format.hpp>
#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/variadic/to_seq.hpp>
#include <boost/preprocessor/facilities/empty.hpp> 
#include <boost/vmd/is_empty.hpp> 
#include <string>
#include <iostream>
#ifdef _MSC_VER
#pragma warning(disable: 4003)
#endif
namespace raft {
    using namespace std;
    namespace detail {
        class LogContextImpl;
    }

    class LogLevel {
    public:
        enum values {
            all,
            debug,
            info,
            warn,
            error,
            fatal,
            off
        };
        LogLevel(values v = off) :value(v) {}
        explicit LogLevel(int v) :value(static_cast<values>(v)) {}
        operator int()const { return value; }
        string ToString()const;
        values value;
    };

    class LogContext : public boost::noncopyable {
    public:
        LogContext(LogLevel ll, const char* file, uint64_t line, const char* method);
        ~LogContext();
        const string &GetFile() const;
        uint64_t      GetLineNumber() const;
        const string &GetMethod() const;
        LogLevel      GetLogLevel() const;
        string        ToString() const;

        std::unique_ptr<detail::LogContextImpl> impl;
    };

    class Logger {
    protected:
        LogLevel logLevel;

    public:
        Logger() :logLevel(LogLevel::all) {}
        virtual void log(const LogContext &ctx, const string &msg) = 0;
        LogLevel setLogLevel(LogLevel lv) { auto o = logLevel; logLevel = lv; return o; }
        LogLevel getLogLevel() { return logLevel; }
    };

    constexpr const char* file_name(const char* file) {
        const char *last_p = nullptr;
        for (const char *p = file; *p; ++p) {
            if (*p == '/' || *p == '\\') {
                last_p = p + 1;
            }
        }
        return last_p ? last_p : file;
    }

#define LOG_FORMAT_ARGS(r, unused, base) % (base)
#define Log(l, level, fmt, ...) \
	(l)->log( \
	LogContext(level, file_name(__FILE__), __LINE__, ""), \
	(boost::format(fmt) BOOST_PP_IF(BOOST_VMD_IS_EMPTY(__VA_ARGS__), \
		BOOST_PP_EMPTY(), \
		BOOST_PP_SEQ_FOR_EACH(LOG_FORMAT_ARGS, v, BOOST_PP_VARIADIC_TO_SEQ(__VA_ARGS__)))).str())

#define iLog(l, fmt, ...) Log(l, LogLevel::info, fmt, __VA_ARGS__)
#define dLog(l, fmt, ...) Log(l, LogLevel::debug, fmt, __VA_ARGS__)
#define wLog(l, fmt, ...) Log(l, LogLevel::warn, fmt, __VA_ARGS__)
#define eLog(l, fmt, ...) Log(l, LogLevel::error, fmt, __VA_ARGS__)
#define fLog(l, fmt, ...) Log(l, LogLevel::fatal, fmt, __VA_ARGS__)

    class DefaultLogger : public Logger {
    public:
        static DefaultLogger &instance() {
            static DefaultLogger _log;
            return _log;
        }
        void log(const LogContext &ctx, const string &msg);
    };
}

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
	) : m_impl(new detail::LogContextImpl) {
		m_impl->level = level;
		m_impl->file = boost::filesystem::path(file).generic_string();
		m_impl->line = line;
		m_impl->method = method;
	}

	LogContext::~LogContext() {
	}

	string LogContext::GetFile() const {
		return m_impl->file;
	}

	uint64_t LogContext::GetLineNumber() const {
		return m_impl->line;
	}

	string LogContext::GetMethod() const {
		return m_impl->method;
	}

	LogLevel LogContext::GetLogLevel() const {
		return m_impl->level;
	}

	string LogContext::ToString() const {
		return m_impl->file + ":" + boost::lexical_cast<string>(m_impl->line) + " " + m_impl->method;
	}

	void DefaultLogger::log(const LogContext &ctx, const string &msg) {
		std::cout
			<< "[" << ctx.GetLogLevel().ToString() << " " << ctx.ToString() << "] "
			<< msg << std::endl;
		if (ctx.GetLogLevel() == LogLevel::fatal) {
			BOOST_THROW_EXCEPTION(std::runtime_error(ctx.ToString() + msg));
		}
	}
}

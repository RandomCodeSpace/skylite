package io.github.randomcodespace.skylite.jdbc;

/**
 * Supplies the bearer token, a JWT, of each request. The driver calls it before every HTTP request it sends and never
 * logs, stores or echoes the token. Name an implementation, which needs a public no-argument constructor, in the
 * connection property {@code tokenProvider}, or put an instance in the {@link java.util.Properties} passed to
 * {@link java.sql.Driver#connect}. Each connection gets its own instance when a class name is given; an
 * implementation that caches a token shares the cache itself.
 */
@FunctionalInterface
public interface TokenProvider {
    /** Returns the current token. An exception fails the request before anything is sent. */
    String token() throws Exception;
}

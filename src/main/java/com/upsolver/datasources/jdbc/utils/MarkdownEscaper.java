package com.upsolver.datasources.jdbc.utils;

/**
 * Utility that escapes special characters of MD format.
 * Current implementation is trivial: it escapes only squire brackets that are only relevant right now.
 * Probably in future other characters will be supported.
 */
public class MarkdownEscaper {
    public static String escape(String md) {
        return md.replace("[", "\\[").replace("]", "\\]");
    }
}

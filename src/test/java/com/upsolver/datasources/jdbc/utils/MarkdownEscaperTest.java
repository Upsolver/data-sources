package com.upsolver.datasources.jdbc.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class MarkdownEscaperTest {
    @Test
    public void empty() {
        assertEquals("", MarkdownEscaper.escape(""));
    }

    @Test
    public void plain() {
        assertEquals("hello world", MarkdownEscaper.escape("hello world"));
    }

    @Test
    public void specialCharacters() {
        assertEquals("foo \\[bar\\]", MarkdownEscaper.escape("foo [bar]"));
    }
}
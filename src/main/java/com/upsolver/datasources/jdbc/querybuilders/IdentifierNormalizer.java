package com.upsolver.datasources.jdbc.querybuilders;

public enum IdentifierNormalizer {
    CASE_INSENSITIVE {
        @Override
        public String normalize(String s) {
            return s;
        }
    },
    TO_UPPER_CASE {
        @Override
        public String normalize(String s) {
            return s == null ? null : s.toUpperCase();
        }
    },
    TO_LOWER_CASE {
        @Override
        public String normalize(String s) {
            return s == null ? null : s.toLowerCase();
        }
    },
    ;
    public abstract String normalize(String s);
}

const keywords = new Set([
    "FROM", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "ON", "USING",
    "WHERE", "FILTER", "EXTEND", "SELECT", "AGGREGATE", "GROUP", "BY", "ORDER",
    "ASC", "DESC", "LIMIT", "OFFSET", "DISTINCT", "AS", "HAVING", "UNION",
    "INTERSECT", "EXCEPT", "CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR",
    "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN", "EXISTS", "DATE", "TRUE", "FALSE",
]);

export const veinPipe = {
    name: "Vein Pipe",
    languageData: { commentTokens: { line: "--" } },
    token(stream) {
        if (stream.eatSpace()) return null;
        if (stream.match("--")) {
            stream.skipToEnd();
            return "comment";
        }
        if (stream.match("|>")) return "operator";
        if (stream.match(/^'(?:[^']|'')*(?:'|$)/)) return "string";
        if (stream.match(/^"(?:[^"]|"")*(?:"|$)/)) return "string";
        if (stream.match(/^(?:\d+\.\d+|\d+)(?:[eE][+-]?\d+)?/)) return "number";
        if (stream.match(/^(?:<=|>=|!=|<>|==|=|<|>|\+|-|\*|\/|%)/)) return "operator";
        if (stream.match(/^[(),.;]/)) return "punctuation";

        const word = stream.match(/^[\p{L}_][\p{L}\p{N}_$]*/u);
        if (word) {
            const value = word[0];
            const upper = value.toUpperCase();
            if (upper === "TRUE" || upper === "FALSE") return "bool";
            if (upper === "NULL") return "null";
            if (keywords.has(upper)) return "keyword";
            return stream.match(/^\s*\(/, false) ? "variableName.function" : "variableName";
        }

        stream.next();
        return null;
    },
};

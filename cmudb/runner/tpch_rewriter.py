from abc import ABC


class Rewriter(ABC):
    def rewrite(self, query_num: int, query_subnum: int, query: str) -> (str, bool):
        raise NotImplementedError("No base implementation.")


class EARewriter(Rewriter):
    def rewrite(self, query_num: int, query_subnum: int, query: str) -> (str, bool):
        is_ea = False
        if query[:10].lower().strip().startswith("select"):
            is_ea, query = True, f"EXPLAIN (ANALYZE, FORMAT JSON, VERBOSE) {query}"
        return query, is_ea


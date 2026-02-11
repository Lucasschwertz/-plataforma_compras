import ast
import unittest
from pathlib import Path


class ProcurementRoutesLayeringTest(unittest.TestCase):
    def test_route_handlers_do_not_embed_sql_or_flow_rules(self) -> None:
        path = Path("app/routes/procurement_routes.py")
        source = path.read_text(encoding="utf-8")
        module = ast.parse(source)
        lines = source.splitlines()

        forbidden_snippets = (
            "db.execute(",
            "flow_action_allowed(",
            "fetch_erp_records(",
        )

        for node in module.body:
            if not isinstance(node, ast.FunctionDef):
                continue
            decorator_src = "\n".join(lines[d.lineno - 1] for d in node.decorator_list)
            if "@procurement_bp.route" not in decorator_src:
                continue

            body_src = "\n".join(lines[node.lineno - 1 : node.end_lineno])
            for snippet in forbidden_snippets:
                self.assertNotIn(
                    snippet,
                    body_src,
                    msg=f"Route handler `{node.name}` should not contain `{snippet}`",
                )


if __name__ == "__main__":
    unittest.main()

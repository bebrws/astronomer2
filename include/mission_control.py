import base64
import hashlib
from collections import Counter
from typing import Tuple

from airflow.sdk import BaseOperator
from airflow.sdk.definitions.context import Context
from airflow.sdk.definitions.dag import DAG


def _b32_short(s: str, n: int = 5) -> str:
    digest = hashlib.sha256(s.encode("utf-8")).digest()
    token = base64.b32encode(digest).decode("ascii").rstrip("=")
    return token[:n]


class MissionControlOperator(BaseOperator):

    def __init__(
        self,
        code_prefix: str = "ORBIT",
        part_len: int = 5,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.code_prefix = code_prefix
        self.part_len = part_len

    def execute(self, context: Context) -> str:
        dag: DAG | None = context.get("dag")
        if dag is None:
            raise ValueError("Operator must be used within a Dag context")

        tasks, edges = self._extract(dag)

        part_types = self._part_types(tasks)
        part_edges = self._part_edges(tasks, edges)
        part_names = self._part_names(tasks)

        code = f"{self.code_prefix}-{part_types}-{part_edges}-{part_names}"
        self.log.info("🔐 Interstellar clearance code: %s", code)
        return code

    def _extract(self, dag: DAG) -> tuple[list[tuple[str, str]], list[Tuple[str, str]]]:
        """
        Returns:
          tasks: [(task_id, operator_class_name)] sorted
          edges: [(upstream_id, downstream_id)] sorted
        Excludes this MissionControl task from both.
        """
        tasks: list[tuple[str, str]] = []
        for task_id, task in dag.task_dict.items():
            if task_id == self.task_id:
                continue
            tasks.append((task_id, type(task).__name__))
        tasks.sort()

        edges: list[Tuple[str, str]] = []
        for task_id, task in dag.task_dict.items():
            if task_id == self.task_id:
                continue
            for upstream_id in sorted(getattr(task, "upstream_task_ids", set())):
                if upstream_id == self.task_id:
                    continue
                edges.append((upstream_id, task_id))
        edges.sort()

        return tasks, edges

    def _part_types(self, tasks: list[tuple[str, str]]) -> str:
        """
        Create code based on operator types.
        """
        counts = Counter(op for _, op in tasks)
        basis = "|".join(f"{op}:{counts[op]}" for op in sorted(counts))
        return _b32_short(basis, self.part_len)

    def _part_edges(self, tasks: list[tuple[str, str]], edges: list[Tuple[str, str]]) -> str:
        """
        Create code based on the graph structure while being tolerant to task ID differences.
        """
        per_type_index: dict[str, int] = {}
        label_for: dict[str, str] = {}

        for task_id, op in tasks:
            per_type_index.setdefault(op, 0)
            per_type_index[op] += 1
            label_for[task_id] = f"{op}#{per_type_index[op]}"

        edge_basis = "|".join(f"{label_for[u]}>{label_for[d]}" for u, d in edges if u in label_for and d in label_for)
        return _b32_short(edge_basis, self.part_len)

    def _part_names(self, tasks: list[tuple[str, str]]) -> str:
        """
        Create code based on task IDs.
        """
        basis = "|".join(tid for tid, _ in tasks)
        return _b32_short(basis, self.part_len)

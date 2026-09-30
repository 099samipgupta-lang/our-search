from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class KnowledgePlanStep:
    step_id: str
    description: str
    action: str
    dependencies: List[str] = field(
        default_factory=list
    )
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class KnowledgePlan:
    plan_id: str
    goal: str
    steps: List[KnowledgePlanStep]
    status: str = "planned"


class KnowledgePlanningEngine:
    VERSION = "knowledge-planning.v1"

    def __init__(self):
        self.plans: Dict[
            str, KnowledgePlan
        ] = {}

    def create(
        self,
        plan_id: str,
        goal: str,
        steps: Optional[
            List[Dict[str, Any]]
        ] = None,
    ) -> KnowledgePlan:
        plan_steps: List[
            KnowledgePlanStep
        ] = []

        for index, item in enumerate(
            steps or [],
            start=1,
        ):
            step_id = str(
                item.get(
                    "step_id",
                    f"{plan_id}:{index}",
                )
            )

            plan_steps.append(
                KnowledgePlanStep(
                    step_id=step_id,
                    description=str(
                        item.get(
                            "description",
                            "",
                        )
                    ).strip(),
                    action=str(
                        item.get(
                            "action",
                            "reason",
                        )
                    ).strip(),
                    dependencies=list(
                        item.get(
                            "dependencies",
                            [],
                        )
                    ),
                    metadata=dict(
                        item.get(
                            "metadata",
                            {},
                        )
                    ),
                )
            )

        plan = KnowledgePlan(
            plan_id=plan_id,
            goal=goal.strip(),
            steps=plan_steps,
        )

        self.plans[plan_id] = plan
        return plan

    def add_step(
        self,
        plan_id: str,
        description: str,
        action: str = "reason",
        dependencies: Optional[
            List[str]
        ] = None,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> Optional[KnowledgePlanStep]:
        plan = self.plans.get(plan_id)

        if plan is None:
            return None

        step_id = (
            f"{plan_id}:"
            f"{len(plan.steps) + 1}"
        )

        step = KnowledgePlanStep(
            step_id=step_id,
            description=description.strip(),
            action=action.strip(),
            dependencies=list(
                dependencies or []
            ),
            metadata=dict(metadata or {}),
        )

        plan.steps.append(step)
        return step

    def next_steps(
        self,
        plan_id: str,
        completed: Optional[
            List[str]
        ] = None,
    ) -> List[KnowledgePlanStep]:
        plan = self.plans.get(plan_id)

        if plan is None:
            return []

        completed_set = set(
            completed or []
        )

        available = []

        for step in plan.steps:
            if step.step_id in completed_set:
                continue

            if all(
                dependency in completed_set
                for dependency
                in step.dependencies
            ):
                available.append(step)

        return available

    def complete(
        self,
        plan_id: str,
    ) -> Optional[KnowledgePlan]:
        plan = self.plans.get(plan_id)

        if plan is None:
            return None

        plan.status = "completed"
        return plan

    def get(
        self,
        plan_id: str,
    ) -> Optional[KnowledgePlan]:
        return self.plans.get(plan_id)

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "plan_count": len(
                self.plans
            ),
            "completed_count": sum(
                plan.status == "completed"
                for plan in self.plans.values()
            ),
        }


__all__ = [
    "KnowledgePlanStep",
    "KnowledgePlan",
    "KnowledgePlanningEngine",
]

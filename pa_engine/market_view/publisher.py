"""One-source publication and batch subscriber isolation for G021."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from .models import MarketViewError, SharedMarketView


Subscriber = Callable[[SharedMarketView], Any]


@dataclass(frozen=True, slots=True)
class PublishedView:
    view: SharedMarketView
    source_generation: int

    @property
    def view_id(self) -> str:
        return self.view.view_id

    @property
    def view_hash(self) -> str:
        return self.view.view_hash

    def to_dict(self) -> dict[str, Any]:
        value = self.view.to_dict()
        value["source_generation"] = self.source_generation
        return value


class SharedMarketViewPublisher:
    """Publish one immutable view to every subscriber in a scheduling batch.

    ``publish`` accepts an already-built view, while ``publish_from`` invokes
    a builder exactly once and fans that same immutable object to all
    subscribers.  No subscriber gets a second source subscription.
    """

    def __init__(self, builder: Any | None = None) -> None:
        self.builder = builder
        self._subscribers: list[Subscriber] = []
        self._published: PublishedView | None = None
        self._generation = 0
        self.source_subscription_count = 0

    @property
    def current(self) -> SharedMarketView | None:
        return self._published.view if self._published is not None else None

    @property
    def last(self) -> SharedMarketView | None:
        return self.current

    def subscribe(self, subscriber: Subscriber, *, replay: bool = True) -> Callable[[], None]:
        if not callable(subscriber):
            raise TypeError("subscriber must be callable")
        self._subscribers.append(subscriber)
        if replay and self._published is not None:
            subscriber(self._published.view)

        def unsubscribe() -> None:
            try:
                self._subscribers.remove(subscriber)
            except ValueError:
                pass

        return unsubscribe

    add_subscriber = subscribe
    register = subscribe

    def publish(self, view: SharedMarketView, *, subscribers: Iterable[Subscriber] | None = None) -> SharedMarketView:
        if not isinstance(view, SharedMarketView):
            raise TypeError("publisher accepts a SharedMarketView")
        self._generation += 1
        self._published = PublishedView(view, self._generation)
        targets = list(self._subscribers if subscribers is None else subscribers)
        for subscriber in targets:
            subscriber(view)
        return view

    def publish_from(self, *args: Any, subscribers: Iterable[Subscriber] | None = None, **kwargs: Any) -> SharedMarketView:
        if self.builder is None:
            raise MarketViewError("missing_builder", "publisher has no view builder")
        self.source_subscription_count += 1
        view = self.builder.build(*args, **kwargs) if hasattr(self.builder, "build") else self.builder(*args, **kwargs)
        if not isinstance(view, SharedMarketView):
            raise MarketViewError("invalid_builder_result", "builder did not return a shared market view")
        # One construction and one source-generation ID for the entire batch.
        return self.publish(view, subscribers=subscribers)

    publish_batch = publish_from
    publish_once = publish_from

    def read(self) -> SharedMarketView | None:
        return self.current

    get = read

    def read_dict(self) -> dict[str, Any] | None:
        return copy.deepcopy(self.current.to_dict()) if self.current is not None else None


Publisher = SharedMarketViewPublisher
SharedMarketViewStore = SharedMarketViewPublisher


__all__ = ["PublishedView", "Publisher", "SharedMarketViewPublisher", "SharedMarketViewStore"]

from ranking.stage8_signals import Stage8SignalEngine
from ranking.scorer import RankingScorer


class RankingEngine:

    def __init__(
        self,
        index_source,
        scorer=None,
        stage8_signals=None,
    ):
        self.index = index_source

        self.scorer = (
            scorer
            if scorer is not None
            else RankingScorer()
        )

        self.stage8_signals = (
            stage8_signals
            if stage8_signals is not None
            else Stage8SignalEngine()
        )

        self._average_length_cache = None
        self._average_length_document_count = None

    def _document_count(self):
        method = getattr(
            self.index,
            "document_count",
            None,
        )

        if callable(method):
            return int(method())

        method = getattr(
            self.index,
            "total_documents",
            None,
        )

        if callable(method):
            return int(method())

        return 0

    def _document_info(self, document_id):
        return self.index.get_document_info(
            document_id
        )

    def _distributed_document_store(self, document_id):
        """
        Return the durable document-store record when the index exposes
        Stage 6 DistributedIndex's shard structure.
        """
        shards = getattr(
            self.index,
            "shards",
            None,
        )

        shard_for_document = getattr(
            self.index,
            "shard_for_document",
            None,
        )

        if not isinstance(shards, dict):
            return None

        if not callable(shard_for_document):
            return None

        try:
            shard_id = shard_for_document(
                document_id
            )

            pipeline = shards.get(shard_id)

            if pipeline is None:
                return None

            store = getattr(
                pipeline,
                "document_store",
                None,
            )

            if store is None:
                return None

            get_method = getattr(
                store,
                "get",
                None,
            )

            if not callable(get_method):
                return None

            return get_method(
                document_id
            )
        except Exception:
            return None

    def _document_metadata(self, document_id):
        record = self._distributed_document_store(
            document_id
        )

        if isinstance(record, dict):
            return record

        get_document_info = getattr(
            self.index,
            "get_document_info",
            None,
        )

        if callable(get_document_info):
            try:
                record = get_document_info(
                    document_id
                )

                if isinstance(record, dict):
                    return record
            except Exception:
                pass

        return {}

    def _active_document_lengths(self):
        """
        Collect lengths of currently active documents.

        Stage 6 DistributedIndex exposes durable per-shard version state,
        so this avoids counting stale/deleted historical segment records.
        """
        shards = getattr(
            self.index,
            "shards",
            None,
        )

        if isinstance(shards, dict):
            lengths = []

            for pipeline in shards.values():
                version_state = getattr(
                    pipeline,
                    "version_state",
                    None,
                )

                search_index = getattr(
                    pipeline,
                    "search_index",
                    None,
                )

                if (
                    version_state is None
                    or search_index is None
                ):
                    continue

                active_ids_method = getattr(
                    version_state,
                    "active_document_ids",
                    None,
                )

                if not callable(active_ids_method):
                    continue

                for document_id in active_ids_method():
                    info = search_index.get_document_info(
                        document_id
                    )

                    if info is not None:
                        lengths.append(
                            max(
                                0,
                                int(
                                    info.get(
                                        "length",
                                        0,
                                    )
                                ),
                            )
                        )

            return lengths

        version_state = getattr(
            self.index,
            "version_state",
            None,
        )

        active_ids_method = getattr(
            version_state,
            "active_document_ids",
            None,
        )

        if callable(active_ids_method):
            lengths = []

            for document_id in active_ids_method():
                info = self.index.get_document_info(
                    document_id
                )

                if info is not None:
                    lengths.append(
                        max(
                            0,
                            int(
                                info.get(
                                    "length",
                                    0,
                                )
                            ),
                        )
                    )

            return lengths

        return []

    def _average_document_length(self):
        document_count = self._document_count()

        if document_count <= 0:
            return 1.0

        if (
            self._average_length_cache is not None
            and self._average_length_document_count
            == document_count
        ):
            return self._average_length_cache

        lengths = self._active_document_lengths()

        if lengths:
            average = sum(lengths) / len(lengths)
        else:
            average = 1.0

        if average <= 0:
            average = 1.0

        self._average_length_cache = float(
            average
        )
        self._average_length_document_count = (
            document_count
        )

        return self._average_length_cache

    def _document_frequency(self, term):
        method = getattr(
            self.index,
            "estimate_document_frequency",
            None,
        )

        if callable(method):
            return int(method(term))

        postings = self.index.get_postings(
            term
        )

        return len(postings)

    def _stage8_adjustment(self, document_id):
        document = self._document_metadata(document_id)

        if not document:
            return 1.0

        last_crawled = document.get("last_crawled")

        freshness = self.stage8_signals.freshness(
            last_crawled
        )

        quality = self.stage8_signals.quality(
            document.get("title", ""),
            document.get("text", ""),
        )

        spam = self.stage8_signals.spam(
            document.get("title", ""),
            document.get("text", ""),
            document.get("url", ""),
        )

        trust = self.stage8_signals.trust(
            document.get("url", ""),
            document.get("title", ""),
            document.get("text", ""),
        )

        duplicate_penalty = self.stage8_signals.duplicate_penalty(
            bool(document.get("exact_duplicate")),
            bool(document.get("possible_duplicate")),
        )

        if document.get("active") is False:
            return 0.0

        return (
            1.0
            + 0.10 * freshness
            + 0.10 * quality
            + 0.05 * trust
            - 0.20 * spam
            - 0.15 * duplicate_penalty
        )

    def score_document(
        self,
        document_id,
        query_terms,
        retrieval_result,
        query_text=None,
    ):
        document_info = self._document_info(
            document_id
        )

        if document_info is None:
            return None

        document_count = self._document_count()

        average_length = (
            self._average_document_length()
        )

        document_length = max(
            0,
            int(
                document_info.get(
                    "length",
                    0,
                )
            ),
        )

        matched_terms = list(
            retrieval_result.get(
                "matched_terms",
                [],
            )
        )

        positions_by_term = dict(
            retrieval_result.get(
                "positions",
                {},
            )
        )

        score = 0.0

        for term in query_terms:
            positions = positions_by_term.get(
                term,
                [],
            )

            term_frequency = len(
                positions
            )

            document_frequency = (
                self._document_frequency(
                    term
                )
            )

            score += self.scorer.term_score(
                term_frequency=term_frequency,
                document_frequency=document_frequency,
                document_count=document_count,
                document_length=document_length,
                average_document_length=average_length,
            )

        score += self.scorer.coverage_bonus(
            matched_terms,
            query_terms,
        )

        score += self.scorer.cross_term_proximity_bonus(
            positions_by_term,
            query_terms,
        )

        metadata = self._document_metadata(
            document_id
        )

        title = metadata.get(
            "title",
            "",
        )

        url = metadata.get(
            "url",
            "",
        )

        if query_text is not None:
            score += 8.0 * self.scorer.exact_title_bonus(
                query_text,
                title,
            )

        score += 4.0 * self.scorer.title_score(
            query_terms,
            title,
        )

        score += 3.0 * self.scorer.url_score(
            query_terms,
            url,
        )

        score *= self._stage8_adjustment(document_id)

        # UNIFIED_FIELD_PROVENANCE_RANKING
        # Rank documents according to where the query actually occurs.
        # Title/URL representation is stronger than incidental body matches.

        fields = document_info.get("fields", {})

        if isinstance(fields, dict) and fields:
            title_terms = fields.get("title", {})
            url_terms = fields.get("url", {})
            domain_terms = fields.get("domain", {})
            canonical_terms = fields.get("canonical_url", {})
            body_terms = fields.get("body", {})
            heading_terms = fields.get("headings", {})
            navigation_terms = fields.get("navigation", {})
            footer_terms = fields.get("footer", {})
            sidebar_terms = fields.get("sidebar", {})
            anchor_terms = fields.get("anchor_text", {})

            if not isinstance(title_terms, dict):
                title_terms = {}
            if not isinstance(url_terms, dict):
                url_terms = {}
            if not isinstance(domain_terms, dict):
                domain_terms = {}
            if not isinstance(canonical_terms, dict):
                canonical_terms = {}
            if not isinstance(body_terms, dict):
                body_terms = {}
            if not isinstance(heading_terms, dict):
                heading_terms = {}
            if not isinstance(navigation_terms, dict):
                navigation_terms = {}
            if not isinstance(footer_terms, dict):
                footer_terms = {}
            if not isinstance(sidebar_terms, dict):
                sidebar_terms = {}
            if not isinstance(anchor_terms, dict):
                anchor_terms = {}

            query_terms_normalized = [
                term
                for term in query_terms
                if isinstance(term, str) and term
            ]

            query_count = max(1, len(query_terms_normalized))

            title_hits = sum(
                1
                for term in query_terms_normalized
                if term in title_terms
            )

            url_hits = sum(
                1
                for term in query_terms_normalized
                if term in url_terms or term in canonical_terms
            )

            domain_hits = sum(
                1
                for term in query_terms_normalized
                if term in domain_terms
            )

            heading_hits = sum(
                1
                for term in query_terms_normalized
                if term in heading_terms
            )

            body_hits = sum(
                1
                for term in query_terms_normalized
                if term in body_terms
            )

            anchor_hits = sum(
                1
                for term in query_terms_normalized
                if term in anchor_terms
            )

            navigation_hits = sum(
                1
                for term in query_terms_normalized
                if term in navigation_terms
            )

            footer_hits = sum(
                1
                for term in query_terms_normalized
                if term in footer_terms
            )

            sidebar_hits = sum(
                1
                for term in query_terms_normalized
                if term in sidebar_terms
            )

            # QUERY COVERAGE GATE
            #
            # A multi-word query must not return a document merely
            # because one incidental word appears in its body.
            #
            # Single-word queries remain unchanged.
            # Strong title/URL/canonical matches are preserved.
            if query_count > 1:
                matched_query_terms = sum(
                    1
                    for term in query_terms_normalized
                    if (
                        term in title_terms
                        or term in url_terms
                        or term in domain_terms
                        or term in canonical_terms
                        or term in heading_terms
                        or term in body_terms
                        or term in anchor_terms
                    )
                )

                minimum_coverage = (
                    (query_count + 1) // 2
                )

                strong_representation = (
                    title_hits > 0
                    or url_hits > 0
                    or domain_hits > 0
                )

                if (
                    matched_query_terms < minimum_coverage
                    and not strong_representation
                ):
                    return None

            normalized_query = (
                query_text.strip().casefold()
                if isinstance(query_text, str)
                else ""
            )

            normalized_title = (
                title.casefold()
                if isinstance(title, str)
                else ""
            )

            normalized_url = (
                url.casefold()
                if isinstance(url, str)
                else ""
            )

            # Exact title representation.
            if (
                normalized_query
                and normalized_query == normalized_title
            ):
                score *= 8.0

            # Full query represented in the title.
            elif title_hits == query_count:
                score *= 5.0

            # Query represented directly by URL/canonical URL.
            elif (
                url_hits == query_count
                or (
                    normalized_query
                    and normalized_query in normalized_url
                )
            ):
                score *= 4.0

            # Partial title representation.
            elif title_hits > 0:
                score *= 2.5

            # Partial URL representation.
            elif url_hits > 0:
                score *= 2.0

            # Strong domain representation.
            elif domain_hits == query_count:
                score *= 4.0

            # Partial domain representation.
            elif domain_hits > 0:
                score *= 2.0

            # Strong structural representation in headings.
            elif heading_hits > 0:
                score *= 2.0

            # Main page content is a normal relevance signal.
            elif body_hits > 0:
                score *= 1.0

            # Anchor text provides useful contextual evidence,
            # but is weaker than the destination page itself.
            elif anchor_hits > 0:
                score *= 0.75

            # Site navigation is usually repeated boilerplate.
            elif navigation_hits > 0:
                score *= 0.25

            # Sidebars are secondary page content.
            elif sidebar_hits > 0:
                score *= 0.20

            # Footer matches are usually site-wide boilerplate.
            elif footer_hits > 0:
                score *= 0.05

            # Defensive fallback for legacy documents.
            else:
                score *= 0.25

        return {
            "document_id": document_id,
            "score": float(score),
            "matched_terms": matched_terms,
            "document_length": document_length,
        }

    def rank(
        self,
        query_terms,
        candidates,
        query_text=None,
    ):
        if not isinstance(query_terms, (list, tuple)):
            query_terms = list(query_terms)

        results = []

        # STRICT SEARCH CANDIDATE GUARD
        #
        # Ranking must never manufacture relevance for a document.
        # A document is eligible only when retrieval has actually
        # matched at least one query term.
        #
        # This prevents unrelated legacy/fallback documents from
        # appearing for queries that have no real index match.

        normalized_query_terms = {
            term.casefold()
            for term in query_terms
            if isinstance(term, str) and term.strip()
        }

        for document_id, retrieval_result in (
            candidates.items()
        ):
            if not isinstance(retrieval_result, dict):
                continue

            matched_terms = retrieval_result.get(
                "matched_terms",
                []
            )

            if not isinstance(matched_terms, (list, tuple, set)):
                continue

            normalized_matched_terms = {
                term.casefold()
                for term in matched_terms
                if isinstance(term, str) and term.strip()
            }

            # No actual query-term intersection means this
            # document must never reach ranking.
            if not (
                normalized_query_terms
                & normalized_matched_terms
            ):
                continue

            print(
                "RANK_DEBUG:",
                document_id,
                "query_terms=",
                query_terms,
                "matched_terms=",
                matched_terms,
                flush=True,
            )

            scored = self.score_document(
                document_id=document_id,
                query_terms=query_terms,
                retrieval_result=retrieval_result,
                query_text=query_text,
            )

            if scored is not None:
                results.append(
                    scored
                )

        results.sort(
            key=lambda item: (
                -item["score"],
                -len(item["matched_terms"]),
                item["document_id"],
            )
        )

        return results

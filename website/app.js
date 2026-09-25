(function () {
    "use strict";

    /*
     * HOMEPAGE BEHAVIOR
     * Keep the original homepage search-button behavior unchanged.
     */
    document.addEventListener(
        "click",
        function (event) {
            var searchButton =
                event.target.closest(".main-search");

            if (!searchButton) {
                return;
            }

            event.preventDefault();
            event.stopPropagation();

            window.location.href = "/search.html";
        },
        true
    );


    /*
     * SEARCH PAGE BEHAVIOR
     */

    function escapeHtml(value) {
        return String(value || "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }


    function getSearchQuery() {
        var params =
            new URLSearchParams(
                window.location.search
            );

        return params.get("q") || "";
    }


    async function performSearch(query) {
        var resultsContainer =
            document.getElementById(
                "search-results"
            );

        var intro =
            document.getElementById(
                "search-intro"
            );

        if (!resultsContainer) {
            return;
        }

        if (!query.trim()) {
            return;
        }

        if (intro) {
            intro.style.display = "none";
        }

        resultsContainer.innerHTML =
            "<p>Searching...</p>";

        try {
            var response =
                await fetch(
                    "/search?q=" +
                    encodeURIComponent(query)
                );

            if (!response.ok) {
                throw new Error(
                    "Search request failed: " +
                    response.status
                );
            }

            var data =
                await response.json();

            var searchData =
                data.results || data;

            var results =
                searchData.results || [];


            if (results.length === 0) {
                resultsContainer.innerHTML =
                    "<div class=\"search-no-results\">" +
                    "<h2>No results found</h2>" +
                    "<p>Our Search couldn't find anything for <strong>" +
                    escapeHtml(query) +
                    "</strong>.</p>" +
                    "</div>";

                return;
            }


            var html = "";

            html +=
                "<div class=\"search-results-header\">" +
                "<p>" +
                results.length +
                " results</p>" +
                "</div>";


            results.forEach(
                function (result) {
                    var title =
                        result.title ||
                        "Untitled";

                    var url =
                        result.url ||
                        "#";

                    var snippet =
                        result.snippet ||
                        "";


                    html +=
                        "<article class=\"search-result\">" +

                        "<a class=\"search-result-url\" " +
                        "href=\"" +
                        escapeHtml(url) +
                        "\" " +
                        "target=\"_blank\" " +
                        "rel=\"noopener noreferrer\">" +
                        escapeHtml(url) +
                        "</a>" +

                        "<h2>" +
                        "<a href=\"" +
                        escapeHtml(url) +
                        "\" " +
                        "target=\"_blank\" " +
                        "rel=\"noopener noreferrer\">" +
                        escapeHtml(title) +
                        "</a>" +
                        "</h2>" +

                        "<p class=\"search-result-snippet\">" +
                        escapeHtml(snippet) +
                        "</p>" +

                        "</article>";
                }
            );


            resultsContainer.innerHTML =
                html;

        } catch (error) {
            console.error(
                "OUR SEARCH error:",
                error
            );

            resultsContainer.innerHTML =
                "<div class=\"search-error\">" +
                "<h2>Search error</h2>" +
                "<p>Something went wrong while searching.</p>" +
                "</div>";
        }
    }


    /*
     * SEARCH PAGE INITIALIZATION
     */
    document.addEventListener(
        "DOMContentLoaded",
        function () {

            var searchForm =
                document.querySelector(
                    ".search-page-form"
                );


            if (searchForm) {
                searchForm.addEventListener(
                    "submit",
                    function (event) {
                        event.preventDefault();

                        var input =
                            document.getElementById(
                                "search-input"
                            );

                        var query =
                            input
                                ? input.value.trim()
                                : "";

                        if (!query) {
                            return;
                        }

                        window.location.href =
                            "/search.html?q=" +
                            encodeURIComponent(
                                query
                            );
                    }
                );
            }


            var query =
                getSearchQuery();


            var input =
                document.getElementById(
                    "search-input"
                );


            if (input && query) {
                input.value = query;
            }


            performSearch(query);
        }
    );

})();

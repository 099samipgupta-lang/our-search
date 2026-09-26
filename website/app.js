(function () {
    "use strict";

    /*
     * HOMEPAGE BEHAVIOR
     * Open the search page when the homepage search area is clicked.
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
     *
     * The server handles the actual search:
     *
     * /search?q=Stanford
     *
     * website_server.py returns the complete HTML
     * results page, so no fetch() or JSON handling
     * is needed here.
     */

    document.addEventListener(
        "DOMContentLoaded",
        function () {

            var searchForm =
                document.querySelector(
                    ".search-page-form"
                );

            if (!searchForm) {
                return;
            }

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
                        "/search?q=" +
                        encodeURIComponent(query);
                }
            );

        }
    );

})();

(function () {
    "use strict";

    function openSearchPage() {
        fetch("/search.html")
            .then(function (response) {
                if (!response.ok) {
                    throw new Error(
                        "Search page returned HTTP " + response.status
                    );
                }

                return response.text();
            })
            .then(function (html) {
                var parser = new DOMParser();

                var newDocument = parser.parseFromString(
                    html,
                    "text/html"
                );

                document.title = newDocument.title;

                document.body.replaceWith(
                    newDocument.body
                );

                document.body.className = "search-page";

                window.history.pushState(
                    { ourSearch: true },
                    "",
                    "/search.html"
                );

                window.scrollTo(0, 0);

                var input =
                    document.getElementById("search-input");

                if (input) {
                    setTimeout(function () {
                        input.focus();
                    }, 50);
                }
            })
            .catch(function (error) {
                console.error(
                    "Our Search navigation error:",
                    error
                );
            });
    }

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

            openSearchPage();
        },
        true
    );
})();

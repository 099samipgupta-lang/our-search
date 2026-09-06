(function () {
    "use strict";

    var searchBar = document.querySelector(".main-search");

    if (!searchBar) {
        return;
    }

    searchBar.addEventListener("click", function (event) {
        event.preventDefault();
        event.stopPropagation();

        fetch("/search.html")
            .then(function (response) {
                if (!response.ok) {
                    throw new Error("Unable to load search page");
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

                document.head.innerHTML = newDocument.head.innerHTML;
                document.body.innerHTML = newDocument.body.innerHTML;

                window.history.pushState(
                    {},
                    "",
                    "/search.html"
                );

                window.scrollTo(0, 0);

                var input = document.getElementById("search-input");

                if (input) {
                    input.focus();
                }
            })
            .catch(function (error) {
                console.error(
                    "Our Search page transition failed:",
                    error
                );
            });
    });
})();

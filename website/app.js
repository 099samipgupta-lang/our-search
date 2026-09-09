(function () {
    "use strict";

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
})();

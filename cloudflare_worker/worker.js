export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response(
        `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Our Search</title>
<link rel="stylesheet" href="/style.css">
</head>
<body>
<main class="page">
<section class="hero">
<h1>Our Search</h1>
<form class="search-box" method="GET" action="/search">
<input name="q" type="search" placeholder="What do you want to know?" autocomplete="off">
<button type="submit">Search</button>
</form>
</section>
</main>
</body>
</html>`,
        {
          headers: {
            "Content-Type": "text/html; charset=utf-8"
          }
        }
      );
    }

    return new Response("Our Search Worker is running.");
  }
};

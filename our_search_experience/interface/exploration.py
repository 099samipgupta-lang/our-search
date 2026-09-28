import html


def build_exploration_experience(query):
    safe_query = html.escape(str(query or "").strip())

    if not safe_query:
        return ""

    return f"""
    <section class="our-experience" aria-label="Our Search exploration">

        <div class="our-experience-card">

            <div class="our-experience-label">
                Explore
            </div>

            <h1 class="our-experience-title">
                {safe_query}
            </h1>

            <div class="our-experience-visual">
                <div class="our-experience-visual-content">
                    <div class="our-experience-visual-symbol">
                        ◉
                    </div>

                    <div class="our-experience-visual-text">
                        Exploration space for this topic
                    </div>
                </div>
            </div>

            <div class="our-experience-actions">
                <button class="our-experience-action" type="button">
                    ▶ Watch
                </button>

                <button class="our-experience-action" type="button">
                    🔊 Listen
                </button>

                <button class="our-experience-action" type="button">
                    📖 Read
                </button>
            </div>

            <section class="our-experience-section">
                <h2 class="our-experience-heading">
                    The Core Idea
                </h2>

                <p class="our-experience-text">
                    Our Search is preparing an exploration of this topic.
                    The experience layer will progressively connect
                    information, explanations, media, and related ideas.
                </p>
            </section>

            <section class="our-experience-section">
                <h2 class="our-experience-heading">
                    Explore
                </h2>

                <div class="our-experience-explore">
                    <div class="our-experience-topic">
                        Related ideas
                    </div>

                    <div class="our-experience-topic">
                        Key concepts
                    </div>

                    <div class="our-experience-topic">
                        Deeper exploration
                    </div>
                </div>
            </section>

        </div>

    </section>
    """


__all__ = ["build_exploration_experience"]

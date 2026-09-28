import html

from our_search_experience.brain.experience_content import (
    ExplorationContent,
)


def render_exploration_experience(
    content: ExplorationContent,
) -> str:

    title = html.escape(content.title)
    visual_title = html.escape(content.visual_title)
    visual_description = html.escape(
        content.visual_description
    )
    core_idea = html.escape(content.core_idea)

    topics_html = []

    for topic in content.related_topics:
        topic_title = html.escape(topic.title)
        topic_description = html.escape(
            topic.description
        )

        topics_html.append(
            f"""
            <article class="our-experience-topic">
                <strong>{topic_title}</strong>
                <span>{topic_description}</span>
            </article>
            """
        )

    return f"""
    <section
        class="our-experience"
        aria-label="Our Search exploration"
    >
        <div class="our-experience-card">

            <div class="our-experience-label">
                Exploration
            </div>

            <h1 class="our-experience-title">
                {title}
            </h1>

            <div class="our-experience-visual">
                <div class="our-experience-visual-content">
                    <div class="our-experience-visual-symbol">
                        ◉
                    </div>

                    <strong>
                        {visual_title}
                    </strong>

                    <div class="our-experience-visual-text">
                        {visual_description}
                    </div>
                </div>
            </div>

            <div class="our-experience-actions">
                <button
                    class="our-experience-action"
                    type="button"
                >
                    ▶ Watch
                </button>

                <button
                    class="our-experience-action"
                    type="button"
                >
                    🔊 Listen
                </button>

                <button
                    class="our-experience-action"
                    type="button"
                >
                    📖 Read
                </button>
            </div>

            <section class="our-experience-section">
                <h2 class="our-experience-heading">
                    The Core Idea
                </h2>

                <p class="our-experience-text">
                    {core_idea}
                </p>
            </section>

            <section class="our-experience-section">
                <h2 class="our-experience-heading">
                    Explore
                </h2>

                <div class="our-experience-explore">
                    {"".join(topics_html)}
                </div>
            </section>

        </div>
    </section>
    """


__all__ = [
    "render_exploration_experience",
]

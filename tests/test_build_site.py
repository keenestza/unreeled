import tempfile
import unittest
from pathlib import Path

from scripts import build_site


FAILING_TITLE = (
    "筑牢头条流量底层基础在线全自动下单拉高头条展现增加作品在推荐页面的整体露出频次"
    "直播间撬动公域流量自动化平台助力视频号爱心占榜依靠榜单优势获取更多系统自然推送"
)


class ReleaseSlugTests(unittest.TestCase):
    def test_short_slug_keeps_existing_url_shape(self):
        page_slug = build_site.make_release_page_slug(
            "2026-07-28",
            "movie",
            "Spider-Man: No Way Home",
        )

        self.assertEqual(
            page_slug,
            "2026-07-28-movie-spiderman-no-way-home",
        )

    def test_long_unicode_slug_is_deterministic_and_filename_safe(self):
        first = build_site.make_release_page_slug(
            "2026-07-28",
            "podcast",
            FAILING_TITLE,
        )
        second = build_site.make_release_page_slug(
            "2026-07-28",
            "podcast",
            FAILING_TITLE,
        )

        self.assertEqual(first, second)
        self.assertLessEqual(
            len(f"{first}.html".encode("utf-8")),
            build_site.MAX_RELEASE_FILENAME_BYTES,
        )
        self.assertRegex(first, r"-[0-9a-f]{16}$")

    def test_long_slugs_with_the_same_prefix_do_not_collide(self):
        shared_prefix = "流量自动化平台" * 30
        first = build_site.make_release_page_slug(
            "2026-07-28",
            "podcast",
            f"{shared_prefix}甲",
        )
        second = build_site.make_release_page_slug(
            "2026-07-28",
            "podcast",
            f"{shared_prefix}乙",
        )

        self.assertNotEqual(first, second)

    def test_release_page_generation_handles_long_unicode_collisions(self):
        releases = [
            {
                "title": FAILING_TITLE,
                "media_type": "podcast",
                "synopsis": "Regression fixture",
                "genres": [],
                "poster_url": "",
                "metadata": {},
            },
            {
                "title": f"{FAILING_TITLE}不同结尾",
                "media_type": "podcast",
                "synopsis": "Collision fixture",
                "genres": [],
                "poster_url": "",
                "metadata": {},
            },
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            docs_dir = Path(temp_dir)
            count = build_site.generate_release_pages(
                {"2026-07-28": {"releases": releases}},
                docs_dir,
            )
            generated = list((docs_dir / "r").glob("*.html"))

            self.assertEqual(count, 2)
            self.assertEqual(len(generated), 2)
            self.assertTrue(
                all(
                    len(path.name.encode("utf-8"))
                    <= build_site.MAX_RELEASE_FILENAME_BYTES
                    for path in generated
                )
            )


if __name__ == "__main__":
    unittest.main()

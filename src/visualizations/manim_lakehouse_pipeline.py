"""
ManimCE Architectural Animations for Lakehouse & Streaming Platform.

This script defines mathematical and architectural procedural animations for:
1. LakehouseMedallionScene: End-to-end data flow (Tier 1 -> Tier 4) with DLQ quarantine.
2. IcebergCompactionScene: Resolving the small-file problem (24 files -> 2 files) via PySpark.
3. EventTimeWatermarkScene: Event-time tumbling windows with advancing watermarks and late data handling.

To render these scenes (requires `pip install manim` and `ffmpeg`):
    manim -pqh src/visualizations/manim_lakehouse_pipeline.py LakehouseMedallionScene
    manim -pqh src/visualizations/manim_lakehouse_pipeline.py IcebergCompactionScene
    manim -pqh src/visualizations/manim_lakehouse_pipeline.py EventTimeWatermarkScene
"""

from __future__ import annotations

import sys

try:
    from manim import (  # type: ignore[import-untyped]
        BLUE,
        BOLD,
        DOWN,
        GRAY,
        GREEN,
        LEFT,
        ORANGE,
        PURPLE,
        RED,
        RIGHT,
        UP,
        WHITE,
        YELLOW,
        Arrow,
        Create,
        DashedLine,
        Dot,
        FadeIn,
        FadeOut,
        Indicate,
        LaggedStart,
        NumberLine,
        Rectangle,
        RoundedRectangle,
        Scene,
        Text,
        Transform,
        VGroup,
    )

    MANIM_AVAILABLE = True
except ImportError:
    MANIM_AVAILABLE = False
    Scene = object  # type: ignore[misc, assignment]


class LakehouseMedallionScene(Scene):  # type: ignore[misc]
    """Cinematic animation of trade flow across the 4-tier medallion lakehouse."""

    def construct(self) -> None:
        if not MANIM_AVAILABLE:
            return

        # Title
        title = Text("Real-Time Lakehouse Medallion Data Flow", font_size=36, weight=BOLD)
        title.to_edge(UP, buff=0.5)
        self.play(FadeIn(title))

        # Define Medallion Tier Nodes
        tier1 = RoundedRectangle(corner_radius=0.15, height=1.6, width=2.4, color=BLUE)
        tier1.set_fill(BLUE, opacity=0.2)
        t1_label = Text("Tier 1: Binance\nRaw Ingest", font_size=16).move_to(tier1)
        g1 = VGroup(tier1, t1_label).shift(LEFT * 4.5 + DOWN * 0.5)

        tier2 = RoundedRectangle(corner_radius=0.15, height=1.6, width=2.4, color=ORANGE)
        tier2.set_fill(ORANGE, opacity=0.2)
        t2_label = Text("Tier 2: Bronze\nIceberg Append", font_size=16).move_to(tier2)
        g2 = VGroup(tier2, t2_label).shift(LEFT * 1.5 + DOWN * 0.5)

        tier3 = RoundedRectangle(corner_radius=0.15, height=1.6, width=2.4, color=GRAY)
        tier3.set_fill(GRAY, opacity=0.2)
        t3_label = Text("Tier 3: Silver\nEnriched & Val", font_size=16).move_to(tier3)
        g3 = VGroup(tier3, t3_label).shift(RIGHT * 1.5 + DOWN * 0.5)

        tier4 = RoundedRectangle(corner_radius=0.15, height=1.6, width=2.4, color=YELLOW)
        tier4.set_fill(YELLOW, opacity=0.2)
        t4_label = Text("Tier 4: Gold\nVWAP Mart", font_size=16).move_to(tier4)
        g4 = VGroup(tier4, t4_label).shift(RIGHT * 4.5 + DOWN * 0.5)

        # DLQ Node (Branching down from Silver)
        dlq_node = RoundedRectangle(corner_radius=0.15, height=1.2, width=2.4, color=RED)
        dlq_node.set_fill(RED, opacity=0.25)
        dlq_label = Text("Dead-Letter\nQueue (DLQ)", font_size=14, color=RED).move_to(dlq_node)
        g_dlq = VGroup(dlq_node, dlq_label).shift(RIGHT * 1.5 + DOWN * 2.5)

        # Flow Arrows
        a1 = Arrow(g1.get_right(), g2.get_left(), buff=0.1, color=WHITE)
        a2 = Arrow(g2.get_right(), g3.get_left(), buff=0.1, color=WHITE)
        a3 = Arrow(g3.get_right(), g4.get_left(), buff=0.1, color=WHITE)
        a_dlq = Arrow(g3.get_bottom(), g_dlq.get_top(), buff=0.1, color=RED)

        # Display Tiers
        self.play(FadeIn(g1), FadeIn(g2), FadeIn(g3), FadeIn(g4), FadeIn(g_dlq))
        self.play(Create(a1), Create(a2), Create(a3), Create(a_dlq))

        # Valid Trade Particle Animation
        valid_dot = Dot(color=GREEN, radius=0.14)
        valid_dot.move_to(g1.get_center())
        valid_tag = Text("Valid Trade ($77,650)", font_size=14, color=GREEN).next_to(valid_dot, UP)

        self.play(FadeIn(valid_dot), FadeIn(valid_tag))
        self.play(
            valid_dot.animate.move_to(g2.get_center()),
            valid_tag.animate.next_to(g2, UP),
            run_time=1.2,
        )
        self.play(
            valid_dot.animate.move_to(g3.get_center()),
            valid_tag.animate.next_to(g3, UP),
            run_time=1.2,
        )
        self.play(
            valid_dot.animate.move_to(g4.get_center()),
            valid_tag.animate.next_to(g4, UP),
            run_time=1.2,
        )
        self.play(Indicate(tier4, color=GREEN))
        self.play(FadeOut(valid_dot), FadeOut(valid_tag))

        # Poison Pill / Corrupt Trade Animation (Routed to DLQ)
        bad_dot = Dot(color=RED, radius=0.16)
        bad_dot.move_to(g1.get_center())
        bad_tag = Text("Poison Pill (Price = -50)", font_size=14, color=RED).next_to(bad_dot, UP)

        self.play(FadeIn(bad_dot), FadeIn(bad_tag))
        self.play(
            bad_dot.animate.move_to(g2.get_center()), bad_tag.animate.next_to(g2, UP), run_time=1.0
        )
        self.play(
            bad_dot.animate.move_to(g3.get_center()), bad_tag.animate.next_to(g3, UP), run_time=1.0
        )
        self.play(Indicate(tier3, color=RED))
        self.play(
            bad_dot.animate.move_to(g_dlq.get_center()),
            bad_tag.animate.next_to(g_dlq, RIGHT),
            run_time=1.2,
        )
        self.play(Indicate(dlq_node, color=RED))
        self.wait(1.5)


class IcebergCompactionScene(Scene):  # type: ignore[misc]
    """Cinematic visualization of small-file consolidation into compacted Parquet splits."""

    def construct(self) -> None:
        if not MANIM_AVAILABLE:
            return

        title = Text("Apache Iceberg Small-File Compaction", font_size=36, weight=BOLD)
        subtitle = Text(
            "24 Small Parquet Fragments (64KB) ➔ 2 Optimized Splits (128MB)",
            font_size=20,
            color=GRAY,
        )
        header = VGroup(title, subtitle).arrange(DOWN, buff=0.2).to_edge(UP, buff=0.5)
        self.play(FadeIn(header))

        # Create 24 small fragmented squares
        small_files = VGroup()
        for row in range(4):
            for col in range(6):
                sq = Rectangle(height=0.45, width=0.45, color=RED)
                sq.set_fill(RED, opacity=0.35)
                sq.move_to(LEFT * 3.5 + RIGHT * (col * 0.55) + UP * (row * 0.55) + DOWN * 1.5)
                small_files.add(sq)

        pre_label = Text("Pre-Compaction: 24 Files (High I/O Overhead)", font_size=18, color=RED)
        pre_label.next_to(small_files, DOWN, buff=0.3)

        self.play(LaggedStart(*[FadeIn(f) for f in small_files], lag_ratio=0.03))
        self.play(FadeIn(pre_label))
        self.wait(1)

        # PySpark Compactor Engine
        compactor_box = RoundedRectangle(corner_radius=0.2, height=1.2, width=3.2, color=PURPLE)
        compactor_box.set_fill(PURPLE, opacity=0.3)
        compactor_txt = Text("PySpark 3.5\n`rewrite_data_files`", font_size=16).move_to(
            compactor_box
        )
        compactor = VGroup(compactor_box, compactor_txt).shift(UP * 0.5)

        self.play(FadeIn(compactor))
        self.play(Indicate(compactor, color=PURPLE))

        # Compact into 2 large consolidated files
        big_file_1 = Rectangle(height=2.2, width=1.8, color=GREEN)
        big_file_1.set_fill(GREEN, opacity=0.4)
        bf1_txt = Text("Compacted\nSplit A\n(128 MB)", font_size=16).move_to(big_file_1)
        g_bf1 = VGroup(big_file_1, bf1_txt).shift(RIGHT * 2.5 + DOWN * 0.8)

        big_file_2 = Rectangle(height=2.2, width=1.8, color=GREEN)
        big_file_2.set_fill(GREEN, opacity=0.4)
        bf2_txt = Text("Compacted\nSplit B\n(128 MB)", font_size=16).move_to(big_file_2)
        g_bf2 = VGroup(big_file_2, bf2_txt).shift(RIGHT * 4.8 + DOWN * 0.8)

        post_label = Text("Post-Compaction: 2 Files (-91.6% File Count)", font_size=18, color=GREEN)
        post_label.next_to(VGroup(g_bf1, g_bf2), DOWN, buff=0.4)

        self.play(Transform(small_files, VGroup(big_file_1, big_file_2)), run_time=2.0)
        self.play(FadeIn(bf1_txt), FadeIn(bf2_txt), Transform(pre_label, post_label))

        # ACID Snapshot pointer swap
        snap_text = Text(
            "ACID Snapshot: 0x9f1a23 ➔ 0xbb8204 (Atomic Metadata Swap)", font_size=18, color=YELLOW
        )
        snap_text.next_to(header, DOWN, buff=0.4)
        self.play(FadeIn(snap_text), Indicate(snap_text, color=YELLOW))
        self.wait(2)


class EventTimeWatermarkScene(Scene):  # type: ignore[misc]
    """Visualizing tumbling windows, advancing watermarks, and late event drop."""

    def construct(self) -> None:
        if not MANIM_AVAILABLE:
            return

        title = Text("Event-Time Watermark & Tumbling Windows", font_size=36, weight=BOLD)
        subtitle = Text(
            "Window Size = 60s | Allowed Lateness (Watermark Delay) = 5s", font_size=18, color=GRAY
        )
        header = VGroup(title, subtitle).arrange(DOWN, buff=0.2).to_edge(UP, buff=0.5)
        self.play(FadeIn(header))

        # Event-Time Axis
        axis = NumberLine(
            x_range=[0, 180, 30],
            length=11,
            color=WHITE,
            include_numbers=True,
            font_size=16,
        ).shift(DOWN * 0.5)
        axis_label = Text("Event Time (Seconds)", font_size=16).next_to(axis, DOWN, buff=0.4)
        self.play(Create(axis), FadeIn(axis_label))

        # Tumbling Window Rectangles
        w1 = Rectangle(height=1.8, width=3.66, color=BLUE).set_fill(BLUE, opacity=0.15)
        w1.move_to(axis.n2p(30) + UP * 1.1)
        w1_lbl = Text("Window 1 [00:00 - 01:00)", font_size=14, color=BLUE).next_to(
            w1, UP, buff=0.15
        )

        w2 = Rectangle(height=1.8, width=3.66, color=BLUE).set_fill(BLUE, opacity=0.15)
        w2.move_to(axis.n2p(90) + UP * 1.1)
        w2_lbl = Text("Window 2 [01:00 - 02:00)", font_size=14, color=BLUE).next_to(
            w2, UP, buff=0.15
        )

        self.play(FadeIn(w1), FadeIn(w1_lbl), FadeIn(w2), FadeIn(w2_lbl))

        # Watermark Line
        wm_line = DashedLine(start=UP * 2.5, end=DOWN * 1.5, color=YELLOW, stroke_width=3)
        wm_line.move_to(axis.n2p(45))
        wm_lbl = Text("Watermark W(t) = max(t) - 5s", font_size=14, color=YELLOW).next_to(
            wm_line, UP
        )

        self.play(Create(wm_line), FadeIn(wm_lbl))

        # Events Arriving
        # Event 1: Normal event inside Window 1
        e1 = Dot(color=GREEN, radius=0.14).move_to(axis.n2p(20) + UP * 0.8)
        self.play(FadeIn(e1))

        # Advance Watermark across Window 1 boundary
        self.play(
            wm_line.animate.move_to(axis.n2p(65)),
            wm_lbl.animate.next_to(axis.n2p(65) + UP * 2.5, UP),
            run_time=1.5,
        )

        # Window 1 Materializes!
        self.play(Indicate(w1, color=GREEN))
        w1_closed = Text("✓ Emitted Candle", font_size=14, color=GREEN).move_to(w1.get_center())
        self.play(FadeIn(w1_closed))

        # Late Event Arriving (t = 50s, but Watermark is now 65s -> Dropped!)
        late_dot = Dot(color=RED, radius=0.14).move_to(axis.n2p(50) + UP * 2.0)
        late_txt = Text("Late Trade (t=50s < W=65s) -> DLQ Drop!", font_size=14, color=RED).next_to(
            late_dot, UP
        )
        self.play(FadeIn(late_dot), FadeIn(late_txt))
        self.play(late_dot.animate.shift(DOWN * 3.5), FadeOut(late_txt), run_time=1.2)
        self.wait(2)


def main() -> None:
    """CLI runner and helper."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    print("=" * 70)
    print(" [*] ManimCE Mathematical Pipeline Animation Suite")
    print("=" * 70)
    if MANIM_AVAILABLE:
        print("[+] Manim Community Edition is detected in your Python environment!")
        print("    Available Scenes:")
        print("      1. LakehouseMedallionScene (4-Tier Flow + DLQ)")
        print("      2. IcebergCompactionScene (Small-file Parquet Consolidation)")
        print("      3. EventTimeWatermarkScene (Advancing Watermark & Tumbling Windows)")
        print("\nTo render in 1080p 60fps:")
        print(
            "    manim -pqh src/visualizations/manim_lakehouse_pipeline.py LakehouseMedallionScene"
        )
    else:
        print("[!] Manim is not installed yet. To install and render high-res videos:")
        print("    pip install manim")
        print("    # Note: Requires ffmpeg installed on your PATH.")
        print("\nOnce installed, render with:")
        print(
            "    manim -pqh src/visualizations/manim_lakehouse_pipeline.py LakehouseMedallionScene"
        )
    print("=" * 70)


if __name__ == "__main__":
    main()

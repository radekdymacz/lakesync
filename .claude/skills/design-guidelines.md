---
name: design-guidelines
description: Minimalistic design system — portable across projects for a consistent look
---

# Design Guidelines

A minimalistic design system. The interface should feel invisible — quiet, fast, and focused entirely on the user's content. This spec is project-agnostic: apply it to any React + Tailwind project for the same result.

## Core Principles

1. **Monochrome chrome** — The UI shell uses only grays. Color is reserved for content: tags, status indicators, data visualization. Primary actions use black/white that invert in dark mode.

2. **Content over chrome** — The interface disappears. Borders are barely visible, shadows are minimal, interactive elements reveal themselves only on hover. The user's content is the focus.

3. **Opacity-based hierarchy** — Text and borders derive from a single base color at varying opacities, not distinct hex values. This produces natural hierarchy without extra hues.

4. **Optimistic and silent** — Actions feel instant via optimistic updates. Success is implied by completion. Never use toast notifications. Errors show inline.

5. **Consistent density** — Compact but not cramped. 14px default text. Spacing on a strict 4px rhythm.

## Tokens

### Colors (CSS Custom Properties)

```css
:root {
  --bg:             #ffffff;
  --bg-secondary:   #f7f6f3;
  --bg-tertiary:    #fbfbfa;
  --bg-hover:       rgba(55, 53, 47, 0.08);
  --bg-active:      rgba(55, 53, 47, 0.16);

  --fg:             #37352f;
  --fg-secondary:   rgba(55, 53, 47, 0.65);
  --fg-tertiary:    rgba(55, 53, 47, 0.5);
  --fg-placeholder:  rgba(55, 53, 47, 0.4);

  --border:         rgba(55, 53, 47, 0.16);
  --border-light:   rgba(55, 53, 47, 0.09);

  --shadow-popup:   0 0 0 1px rgba(15,15,15,0.05),
                    0 3px 6px rgba(15,15,15,0.1),
                    0 9px 24px rgba(15,15,15,0.2);

  --code-bg:        #f7f6f3;
  --selection:      rgba(35, 131, 226, 0.28);
  --focus-ring:     #2383e2;
  --danger:         #eb5757;
}

.dark {
  --bg:             #191919;
  --bg-secondary:   #202020;
  --bg-tertiary:    #252525;
  --bg-hover:       rgba(255, 255, 255, 0.055);
  --bg-active:      rgba(255, 255, 255, 0.1);

  --fg:             rgba(255, 255, 255, 0.9);
  --fg-secondary:   rgba(255, 255, 255, 0.6);
  --fg-tertiary:    rgba(255, 255, 255, 0.4);
  --fg-placeholder:  rgba(255, 255, 255, 0.3);

  --border:         rgba(255, 255, 255, 0.13);
  --border-light:   rgba(255, 255, 255, 0.07);

  --shadow-popup:   0 0 0 1px rgba(255,255,255,0.07),
                    0 3px 6px rgba(0,0,0,0.4),
                    0 9px 24px rgba(0,0,0,0.5);

  --code-bg:        #2f3437;
  --selection:      rgba(35, 131, 226, 0.35);
  --danger:         #ff7b72;
}
```

### Tag Palette (8 colors, light + dark)

For select options, status badges, and labels. Assign by value hash for consistency.

| # | Light BG | Light Text | Dark BG | Dark Text |
|---|----------|------------|---------|-----------|
| 0 | `rgba(227,226,224,0.5)` | `rgb(50,48,44)` | `rgba(90,90,90,0.3)` | `rgba(255,255,255,0.81)` |
| 1 | `rgba(253,236,200,0.7)` | `rgb(64,44,27)` | `rgba(133,76,29,0.4)` | `rgb(255,212,128)` |
| 2 | `rgba(250,222,201,0.7)` | `rgb(73,41,14)` | `rgba(153,85,34,0.4)` | `rgb(255,192,128)` |
| 3 | `rgba(219,237,219,0.7)` | `rgb(28,56,41)` | `rgba(40,109,69,0.4)` | `rgb(127,202,149)` |
| 4 | `rgba(211,229,239,0.7)` | `rgb(24,51,71)` | `rgba(40,87,120,0.4)` | `rgb(128,188,225)` |
| 5 | `rgba(232,222,238,0.7)` | `rgb(65,36,84)` | `rgba(91,59,116,0.4)` | `rgb(192,162,214)` |
| 6 | `rgba(245,224,233,0.7)` | `rgb(76,35,55)` | `rgba(122,54,84,0.4)` | `rgb(230,153,186)` |
| 7 | `rgba(255,226,221,0.7)` | `rgb(93,23,21)` | `rgba(143,48,44,0.4)` | `rgb(255,145,138)` |

### Typography

Font stacks:
- **Sans:** `-apple-system, BlinkMacSystemFont, "Segoe UI", "Liberation Sans", sans-serif`
- **Mono:** `"SFMono-Regular", Menlo, Consolas, "PT Mono", "Liberation Mono", Courier, monospace`

Type scale:

| Size | Usage |
|------|-------|
| 11px | Uppercase section labels, metadata |
| 12px | Tooltips, captions, code in compact contexts |
| 13px | Context menu items, compact secondary text |
| **14px** | **Default** — all interactive UI: inputs, buttons, menus, tables, lists |
| 16px | Page body, long-form content |
| 40px | Page titles |

Weights:
- 400 — Body text, tags
- 500 — Buttons, labels, table headers
- 600 — Headings
- 700 — Page titles only

### Spacing

Built on a **4px base unit**: 4, 8, 12, 16, 24, 32, 48, 96.

| Context | Padding |
|---------|---------|
| List/tree items | 4px vertical, 8px horizontal |
| Menu items | 6px vertical, 12px horizontal |
| Inputs | 0 vertical (height-based), 12px horizontal |
| Table cells | 8px vertical, 10px horizontal |
| Modals/panels | 16–24px |
| Page content area | 96px horizontal |

Gaps: 4px (tight), 8px (normal), 16px (loose).

### Border Radius

Minimal. Signals element hierarchy:

| Radius | Usage |
|--------|-------|
| 3px | Tags, list items, inline code |
| 4px | Inputs, small buttons, menu items, tooltips |
| 6px | Buttons, code blocks, images, popups |
| 8px | Modals, dialogs (maximum) |

### Shadows

Most elements have **no shadow**. Only floating layers get shadows:

- **Popups/menus:** `var(--shadow-popup)` — layered: thin outline + medium + deep spread
- **Everything else:** Use borders or background contrast instead

### Motion

Two timing tiers:
- **120ms** — Direct interactions: hover, active, focus
- **200ms** — Entrances: fade-in, slide-in

Easing: `ease-out` for interactions, `ease-in-out` for loops.

Standard entrance: fade from 0 opacity + 4px translateY. Slide-in: 8px translateX.

### Icons

Use **lucide-react** (or any consistent stroke-icon set).

| Size | Usage |
|------|-------|
| 12px | Inside checkboxes, tiny indicators |
| 16px | **Default** — buttons, menus, inline |
| 20px | Navigation items, larger interactive areas |
| 24px | Empty states, section headers |

Secondary icons use `--fg-secondary`. Hover-reveal icons start at opacity 0, transition to 1 on parent hover.

## Component Conventions

### Buttons
Five variants:
- **Primary** — `bg: black` / `dark: white`, text inverts. The only high-contrast element.
- **Secondary** — `bg: --bg-secondary`, `border: --border`. Subtle.
- **Ghost** — No background. `fg: --fg-secondary`, background appears on hover.
- **Danger** — `bg: red-600`, white text.
- **Link** — Inline text style, no padding.

Four sizes: sm (h28), md (h36), lg (h44), icon (32x32).

### Inputs
Height: 36px. Border: `--border`. Focus: 2px gray ring (monochrome, not blue). Error: red border + red ring. Placeholder: `--fg-placeholder`.

### Checkboxes
16x16px. Unchecked: 1.5px border in `--fg-secondary`. Checked: fills black (light) / white (dark). Icon: checkmark or minus at 12px.

### Menus & Dropdowns
Float with `--shadow-popup`. Min-width 180–220px. Items at 13–14px with hover highlight. Support: section labels (11px uppercase), dividers, keyboard shortcuts, danger items, disabled state.

### Select / Tags
Single and multi-select with inline search and keyboard navigation. Tags are small pills (3px radius) using the 8-color tag palette. Multi-select shows tag pills inline. Color assigned by string hash for deterministic results.

### Navigation / Tree
List items 28px tall. Depth indentation: 12px per level. Icon area: 20x20. Action buttons hidden, revealed on row hover. Active: `--bg-active`. Hover: `--bg-hover`.

### Tables
Header: `--bg-secondary` background, 500-weight text in `--fg-secondary`. Cells: 14px, padded 8/10px. Borders: bottom-only using `--border-light`. Rows highlight on hover.

## Dark Mode

Strategy: Tailwind `class` (or any class-toggle approach).

**Rule: use CSS variables for every color.** They auto-switch. Only use manual dark overrides for the few hardcoded values (e.g., primary button: `bg-gray-900 dark:bg-white`).

Never ship a hardcoded color without its dark counterpart.

## Anti-Patterns

- **No colored buttons** — Primary is black/white, never blue or branded
- **No toasts** — All feedback is inline within the triggering component
- **No heavy shadows** — Only floating layers (menus, modals) get shadows
- **No large radii** — 8px maximum. No pill shapes on containers
- **No visible scrollbars** — Hide globally, content still scrolls
- **No color in navigation** — Nav/sidebar is strictly monochrome
- **No custom brand colors in chrome** — Gray only; color lives in content

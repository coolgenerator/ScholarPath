```markdown
# Design System Document: Scholarship & Institutional Intelligence

## 1. Overview & Creative North Star
**Creative North Star: "The Academic Curator"**
This design system moves away from the cold, mechanical nature of traditional data dashboards. Instead, it adopts the persona of a high-end academic publication or a bespoke concierge service. We treat information not as "data points" but as "knowledge assets." 

To break the "standard SaaS" look, we utilize **Editorial Asymmetry**. By pairing the wide, data-rich dashboard (Right) with a slender, focused conversational panel (Left), we create a functional tension that guides the eye. We avoid the "boxed-in" feeling of generic grids by using expansive white space, over-sized display typography for context, and a physical layering of surfaces.

---

## 2. Colors: Tonal Architecture
Color is used as a functional signal, not just decoration. Our palette is built on **Material 3 Tonal Logic**, ensuring every color has a specific role in the information hierarchy.

### Core Palette
- **Primary (Professional Blue):** `#0040a1` (Primary) for brand presence and `#0056d2` (Primary Container) for interactive zones.
- **Secondary (Warning Orange):** `#904d00` (Secondary) for critical conflicts in scholarship eligibility.
- **Tertiary (Success Green):** `#005312` (Tertiary) for strong school matches and "Recommended" status.
- **Neutral (The Canvas):** `#f8f9fa` (Surface/Background) provides a pristine, paper-like foundation.

### The "No-Line" Rule
**Borders are prohibited for sectioning.** To define the dual-panel architecture (Chat vs. Dashboard), do not draw a line. Instead, use a background shift. The Chat panel should sit on `surface-container-low` (`#f3f4f5`), while the Dashboard workspace occupies the `surface` (`#f8f9fa`).

### Surface Hierarchy & Nesting
Treat the UI as stacked sheets of fine stationery.
- **Level 0 (Base):** `surface`
- **Level 1 (Sections):** `surface-container-low`
- **Level 2 (Cards/Modules):** `surface-container-lowest` (Pure `#ffffff`)
- **Level 3 (Floating/Pop-overs):** `surface-bright` with Glassmorphism.

### The "Glass & Gradient" Rule
To add "soul" to a data-heavy environment, use **Signature Textures**:
- **Primary CTAs:** Apply a subtle linear gradient from `primary` (`#0040a1`) to `primary_container` (`#0056d2`) at 135 degrees.
- **Floating Intelligence:** Use Glassmorphism for "Scholarship Tips" that hover over the dashboard. Apply `surface_container_lowest` at 80% opacity with a `20px` backdrop-blur.

---

## 3. Typography: Editorial Authority
We utilize a dual-font strategy to balance character with extreme legibility.

- **The Display Layer (Manrope):** Used for headlines and large data callouts. Its geometric construction feels modern yet established. 
  - *Example:* Use `display-md` for "98% Match" to make it feel like a headline in a premium journal.
- **The Information Layer (Inter):** Used for all body text, labels, and data tables. Inter is chosen for its high X-height, ensuring scholarship terms and conditions remain legible even at `body-sm` (`0.75rem`).

**Tonal Hierarchy:**
- **Primary Headers:** `headline-sm` in `on_surface` (`#191c1d`).
- **Supportive Metadata:** `label-md` in `on_surface_variant` (`#424654`).

---

## 4. Elevation & Depth
In this design system, depth is a tool for focus, not just aesthetics.

- **The Layering Principle:** Rather than using shadows to define cards, place a `surface-container-lowest` (White) card onto a `surface-container` (`#edeeef`) background. The 2% shift in brightness is enough to signify depth to the human eye.
- **Ambient Shadows:** For high-priority scholarship "Knowledge Cards," use an ambient lift. 
  - *Shadow:* `0px 12px 32px rgba(25, 28, 29, 0.04)`. It should feel like the card is floating on a cushion of air, not casting a harsh shadow.
- **The "Ghost Border" Fallback:** In high-density tables where separation is critical, use a "Ghost Border" of `outline_variant` (`#c3c6d6`) at **15% opacity**.

---

## 5. Components: Knowledge Elements

### Knowledge Cards
*The core unit of the Scholarship Dashboard.*
- **Style:** No borders. Background: `surface-container-lowest`. 
- **Corner Radius:** `xl` (`0.75rem`) for an approachable, modern feel.
- **Spacing:** Use spacing `8` (`1.75rem`) for internal padding to give data "room to breathe."

### Data Tables & Lists
- **Rule:** Forbid the use of horizontal divider lines.
- **Implementation:** Separate rows using a background toggle between `surface` and `surface-container-low`, or simply use vertical white space (spacing `4`). 
- **Header:** Use `label-sm` in `on_surface_variant` with all-caps styling for a professional, "ledger" feel.

### Chat Interface (The Advisor)
- **User Bubbles:** `primary` background with `on_primary` text.
- **System/AI Bubbles:** `surface-container-high` (`#e7e8e9`) with `on_surface`.
- **Interaction:** Messages should use the `lg` (`0.5rem`) roundedness scale, but the "tail" corner should be `sm` (`0.125rem`) to indicate directionality.

### Inputs & Search
- **Field Style:** Minimalist. No bottom line. Use `surface-container-highest` as the field background with a `md` (`0.375rem`) radius.
- **Focus State:** Transition the background to `primary_fixed` (`#dae2ff`) for a soft, glowing highlight rather than a harsh border.

---

## 6. Do’s and Don’ts

### Do:
- **Do** use intentional asymmetry. A large scholarship graph can be slightly offset from the card center to create visual interest.
- **Do** use "Success Green" (`tertiary`) sparingly. It should be a reward for the user finding a high-value scholarship.
- **Do** use `body-lg` for the first paragraph of scholarship descriptions to create an editorial entry point.

### Don't:
- **Don't** use 1px solid black or dark grey borders. Ever.
- **Don't** use standard "Drop Shadows." Only use the Ambient Shadows defined in Section 4.
- **Don't** clutter the screen. If the scholarship data is heavy, increase the spacing scale (e.g., move from spacing `6` to spacing `10`) rather than shrinking the text.
- **Don't** use pure black (`#000000`) for text. Use `on_surface` (`#191c1d`) to maintain a softer, premium contrast.

---
*Note: This system is designed to be living. When in doubt, prioritize the "Academic Curator" mindset: Is this information being presented with clarity, authority, and elegance?*```
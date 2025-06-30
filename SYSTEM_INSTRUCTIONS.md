---
# 📘 system_instructions_for_cursor.md

This file defines enforced architectural guidelines for AI tools (e.g. Cursor, ChatGPT) in our React Native frontend. All code must follow this system prompt. Any violation introduces tech debt or risks production bugs.

---

## ✅ SYSTEM RULES (REQUIRED)

| ✅ Rule | 💡 Why | 📌 Example |
|--------|--------|------------|
| Keep Screens Pure | Prevent bloated files, promote separation of concerns | ❌ `fetchUserAlcohols()` inside `app/alcohols.tsx` → move to `useUserAlcohols()` |
| Extract Business Logic to Hooks | Reusability, testability, separation from UI | ✅ `useSubmitForm()` handles side effects → screen just renders result |
| Enforce Typed Functions | Prevent runtime errors, enable autocomplete | ❌ `any` or missing return types → must use interfaces or `typeof` |
| Centralize Firebase Calls | Avoid duplicating queries or Firestore misuse | ✅ Query goes in `/lib/firebase/useX.ts`, not in screens or components |
| Cap File Size (≤ 500 lines) | Maintain readability, reduce merge conflicts | ❌ 900-line screen → must extract hooks/components |
| Debounce & Paginate Lists | Prevent app crashes, avoid overfetching | ❌ Load entire collection → ✅ `limit(100)` with pagination |
| Reuse Shared Components | Prevent visual drift, simplify design updates | ❌ 3 similar modals in 3 files → ✅ `ModalWrapper` in `/components` |
| Centralize Styling | Consistent spacing/colors, theming support | ✅ use `Colors.PRIMARY` from `/constants/Colors.ts` |
| Guard All Async Flows | Avoid silent failures in UI | ✅ handle `isLoading`, `error`, and empty state for every hook |
| Use Intent-Based Names | Improve searchability and readability | ❌ `handleThing()` → ✅ `submitForm()` or `resetAlcoholState()` |

---

## 🔒 MUST-NOT-BREAK RULES

These rules are **critical** to app stability and developer sanity. All violations are blockers in PR reviews:

1. **No logic or data fetching in screen files.**
2. **No Firestore queries in UI components.**
3. **No `any` or untyped functions.**
4. **No massive lists without pagination.**
5. **No inline async calls inside `render()` blocks.**
6. **No `console.log` in production code.**
7. **All components must handle loading/error/empty UI states.**
8. **Never write listeners without cleanup (`unsubscribe()`).**
9. **No duplicated UI patterns — extract shared components.**
10. **No exposing or leaking Firebase config/API keys in any file.**

---

## 🤖 AI WEAKNESSES & MITIGATIONS

| AI Tends To... | ❌ Problem | ✅ Mitigation |
|----------------|-------------|----------------|
| Dump logic in screen files | Coupled views and state → unreadable files | Move logic to `/hooks/` custom hook |
| Use copy-pasted modals/UI blocks | Duplicates multiply bugs and design drift | Extract `SharedModal`, `ButtonRow` etc. |
| Fetch data in JSX | Causes excessive re-renders and side effects | Use `useX()` before render cycle |
| Write without cleanup | Memory leaks from `onSnapshot`, `setTimeout` | Always return `unsubscribe()` or `clearTimeout()` |
| Skip edge states | Blank UI on error or empty responses | Always show fallback UI with loading/error/empty |
| Load everything at once | Firebase bill spikes, app crashes | Use `.limit()`, `startAfter()`, or `infiniteQuery` |
| Use vague names (`handleThing`) | Untraceable logic | Use domain-specific names with clear intent |
| Leave logic in `useEffect()` soup | Hard to debug, impossible to test | Split into helper functions or move to hooks |

---

## 🧱 FILE STRUCTURE

- `/app`: Screens only — layout and navigation. **No logic.**
- `/components`: Pure UI. Stateless where possible. Shared across app.
- `/hooks`: All stateful logic. Data fetching, mutation, side effects.
- `/lib`: Firebase clients, validators, formatters, helpers.
- `/constants`: Shared values — colors, sizes, keys, enums, roles.

---

## ✍️ INSTRUCTION-WRITING PATTERNS

| 🛠 Rule | 🤔 Why It Exists | ✅ Example |
|--------|------------------|--------------|
| "No fetching in components" | Ensures data flows are testable & isolated | Instead of `useEffect(fetch())` in component → create `useFetchDrinks()` |
| "Name functions clearly" | Makes logic self-documenting | ❌ `handleClick()` → ✅ `deleteDrink()` |
| "Split large files early" | Keeps code navigable, simplifies reviews | If screen hits 300 lines, extract component/hooks |
| "Use fallbacks for async UI" | Avoids blank or broken UIs | Always include `if (isLoading)` / `if (error)` / `if (empty)` |
| "Avoid repeated layouts" | Prevents drift and wasted time | Create `DrinkCard`, `GuideRow`, `ModalWrapper` once and reuse |

---

## 🧠 FINAL NOTE

This file is not optional. Cursor and all AI tools must treat these as enforced rules. PR reviewers will reject code that violates any critical instruction.

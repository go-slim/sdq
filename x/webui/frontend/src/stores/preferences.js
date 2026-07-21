import { computed, reactive } from "vue";

export const supportedColors = ["indigo", "red", "amber", "lime", "emerald", "fuchsia"];
export const supportedLanguages = ["en", "zh"];
export const supportedThemes = ["light", "dark", "system"];
export const defaultPollingInterval = 60000;

const keys = {
  darkColor: "sdq/color/dark",
  language: "sdq/language",
  legacyColor: "sdq/color",
  lightColor: "sdq/color/light",
  pollingInterval: "sdq/pollingInterval",
  sidebar: "sdq/sidebar",
  theme: "sdq/theme",
};

const media = window.matchMedia("(prefers-color-scheme: dark)");
const storedTheme = localStorage.getItem(keys.theme);
const legacyColor = localStorage.getItem(keys.legacyColor);
const fallbackColor = supportedColors.includes(legacyColor) ? legacyColor : "indigo";
const storedDarkColor = localStorage.getItem(keys.darkColor);
const storedLightColor = localStorage.getItem(keys.lightColor);
const storedLanguage = localStorage.getItem(keys.language);

export const preferences = reactive({
  darkColor: supportedColors.includes(storedDarkColor) ? storedDarkColor : fallbackColor,
  language: supportedLanguages.includes(storedLanguage) ? storedLanguage : null,
  lightColor: supportedColors.includes(storedLightColor) ? storedLightColor : fallbackColor,
  pollingInterval: parsePollingInterval(localStorage.getItem(keys.pollingInterval)),
  sidebarCollapsed: (localStorage.getItem(keys.sidebar) || "false") === "true",
  systemDark: media.matches,
  systemLanguage: navigator.language,
  theme: supportedThemes.includes(storedTheme) ? storedTheme : "system",
});

export const isDark = computed(() => preferences.theme === "system"
  ? preferences.systemDark
  : preferences.theme === "dark");

export const activeColor = computed(() => isDark.value
  ? preferences.darkColor
  : preferences.lightColor);

export const activeLanguage = computed(() => {
  if (preferences.language) return preferences.language;
  const language = preferences.systemLanguage.split("-")[0];
  return supportedLanguages.includes(language) ? language : "en";
});

export function setTheme(value) {
  if (!supportedThemes.includes(value)) return;
  preferences.theme = value;
  writePreference(keys.theme, value === "system" ? null : value);
  applyTheme();
}

export function toggleTheme() {
  const index = supportedThemes.indexOf(preferences.theme);
  setTheme(supportedThemes[(index + 1) % supportedThemes.length]);
}

export function setColor(mode, value) {
  if ((mode !== "light" && mode !== "dark") || !supportedColors.includes(value)) return;
  preferences[`${mode}Color`] = value;
  writePreference(keys[`${mode}Color`], value);
  applyTheme();
}

export function setLanguage(value) {
  if (value != null && !supportedLanguages.includes(value)) return;
  preferences.language = value;
  writePreference(keys.language, value);
}

export function setPollingInterval(value) {
  preferences.pollingInterval = parsePollingInterval(value);
  writePreference(keys.pollingInterval, preferences.pollingInterval);
}

export function setSidebarCollapsed(value) {
  preferences.sidebarCollapsed = Boolean(value);
  writePreference(keys.sidebar, preferences.sidebarCollapsed);
}

export function resetPreferences() {
  setTheme("system");
  setColor("light", "indigo");
  setColor("dark", "indigo");
  setLanguage(null);
  setPollingInterval(defaultPollingInterval);
  setSidebarCollapsed(false);
}

export function installPreferences() {
  migrateLegacyColor();
  applyTheme();
  const onThemeChange = (event) => {
    preferences.systemDark = event.matches;
    applyTheme();
  };
  const onLanguageChange = () => {
    preferences.systemLanguage = navigator.language;
  };
  const onStorage = (event) => {
    switch (event.key) {
    case keys.theme:
      preferences.theme = supportedThemes.includes(event.newValue) ? event.newValue : "system";
      applyTheme();
      break;
    case keys.lightColor:
      preferences.lightColor = supportedColors.includes(event.newValue) ? event.newValue : "indigo";
      applyTheme();
      break;
    case keys.darkColor:
      preferences.darkColor = supportedColors.includes(event.newValue) ? event.newValue : "indigo";
      applyTheme();
      break;
    case keys.legacyColor:
      if (supportedColors.includes(event.newValue)) {
        preferences.lightColor = event.newValue;
        preferences.darkColor = event.newValue;
        applyTheme();
      }
      break;
    case keys.language:
      preferences.language = supportedLanguages.includes(event.newValue) ? event.newValue : null;
      break;
    case keys.pollingInterval:
      preferences.pollingInterval = parsePollingInterval(event.newValue);
      break;
    case keys.sidebar:
      preferences.sidebarCollapsed = event.newValue === "true";
      break;
    }
  };

  media.addEventListener("change", onThemeChange);
  window.addEventListener("languagechange", onLanguageChange);
  window.addEventListener("storage", onStorage);
  return () => {
    media.removeEventListener("change", onThemeChange);
    window.removeEventListener("languagechange", onLanguageChange);
    window.removeEventListener("storage", onStorage);
  };
}

function applyTheme() {
  document.documentElement.classList.toggle("dark", isDark.value);
  document.documentElement.classList.toggle("light", !isDark.value);
  document.documentElement.dataset.accent = activeColor.value;
}

function migrateLegacyColor() {
  if (!supportedColors.includes(legacyColor)) return;
  if (!supportedColors.includes(storedLightColor)) {
    writePreference(keys.lightColor, legacyColor);
  }
  if (!supportedColors.includes(storedDarkColor)) {
    writePreference(keys.darkColor, legacyColor);
  }
  localStorage.removeItem(keys.legacyColor);
}

function parsePollingInterval(value) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : defaultPollingInterval;
}

function writePreference(key, value) {
  if (value == null) localStorage.removeItem(key);
  else localStorage.setItem(key, String(value));
}

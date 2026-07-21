import { createApp } from "vue";

import App from "./app.js";
import { installI18n } from "./i18n.js";

const app = createApp(App);
installI18n(app);
app.mount("#app");

import appCss from "./app.css" with { type: "text" };

const appSheet = new CSSStyleSheet();
appSheet.replaceSync(appCss);
document.adoptedStyleSheets.push(appSheet);

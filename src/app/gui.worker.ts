import { Webview } from "@webview/webview";
import { DEV } from "~/env.ts";

/*
    basic GUI window.
    later compiled deno executable can be packaged as appimage, flatpak,
    and other stuff for macos and windows
*/

const webview = new Webview(DEV);
webview.title = "BitcoinAtlas";

webview.navigate(`http://localhost:50021`);
webview.run();
// For now just exit deno, later we might make it like background running thing etc.
Deno.exit(0);

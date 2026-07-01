export type MessagePortLike = {
	postMessage(message: any, transfer?: Transferable[]): void;
	addEventListener(type: "message", listener: (event: MessageEvent) => void, options?: AddEventListenerOptions): void;
	removeEventListener(type: "message", listener: (event: MessageEvent) => void, options?: EventListenerOptions): void;
};

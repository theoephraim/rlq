type Hash = { [key: string]: any };

declare var keydb: {
  // built-in
  log(toLog: string): void,
  call(fnName: string, ...args: (string | number)[]),
  register(fn: (...any) => any),

  // added helper functions - see helpers.ts
  setHash(key: string, hash: Hash): void,
  getHash(key: string): Hash,
};
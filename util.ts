type Reader = Deno.Reader;
type Writer = Deno.Writer;
type Closer = Deno.Closer;
type ReadResult = Deno.ReadResult;
import Buffer = Deno.Buffer;

interface Dialer {
  (): Promise<Reader & Writer & Closer>;
}

interface Auth {
  (conn: Reader & Writer & Closer): Promise<boolean>;
}

interface Scanner {
  Next(): boolean;
  Scan(): Promise<(string | number)[]>;
  Err(): Error;
}

function getDialer(addr: string): Dialer {
  return async function(): Promise<Reader & Writer & Closer> {
    return await Deno.dial("tcp", addr);
  };
}

export function str2ab(str) {
  var ab = new ArrayBuffer(str.length);
  var buf = new Uint8Array(ab);
  for (var i = 0, strLen = str.length; i < strLen; i++) {
    buf[i] = str.charCodeAt(i);
  }
  return buf;
}

export {
  Reader,
  Writer,
  Closer,
  Dialer,
  getDialer,
  ReadResult,
  Buffer,
  Auth,
  Scanner
};

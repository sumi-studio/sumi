import { math as micromarkMath } from "micromark-extension-math";
import { markdownLineEnding } from "micromark-util-character";
import { codes, types } from "micromark-util-symbol";
import type {
  Code,
  Construct,
  Effects,
  State,
  Token,
} from "micromark-util-types";
import type { Processor } from "unified";

const baseTextConstruct = micromarkMath({
  singleDollarTextMath: true,
}).text?.[codes.dollarSign];
const baseSingleDollar = (
  Array.isArray(baseTextConstruct) ? baseTextConstruct[0] : baseTextConstruct
) as Construct;

function whitespace(code: Code): boolean {
  return code === codes.space || markdownLineEnding(code);
}

function digit(code: Code): boolean {
  return code !== null && code >= codes.digit0 && code <= codes.digit9;
}

/**
 * A single-dollar math tokenizer with Pandoc-like boundary admission. When a
 * would-be closer is instead another valid opener, the current construct
 * fails atomically so micromark retries at that later dollar. This prevents a
 * price from consuming later Markdown or a later real formula.
 */
const safeSingleDollar: Construct = {
  ...baseSingleDollar,
  tokenize(effects: Effects, ok: State, nok: State): State {
    const self = this;
    let sizeOpen = 0;
    let sizeClose = 0;
    let beforeClose: Code = codes.eof;
    let sequenceToken: Token;

    return start;

    function start(code: Code): State | undefined {
      if (code !== codes.dollarSign) return nok(code);
      effects.enter("mathText");
      effects.enter("mathTextSequence");
      return sequenceOpen(code);
    }

    function sequenceOpen(code: Code): State | undefined {
      if (code === codes.dollarSign) {
        effects.consume(code);
        sizeOpen += 1;
        return sequenceOpen;
      }
      if (sizeOpen !== 1 || code === codes.eof || whitespace(code)) {
        return nok(code);
      }
      effects.exit("mathTextSequence");
      return between(code);
    }

    function between(code: Code): State | undefined {
      if (code === codes.eof) return nok(code);
      if (code === codes.dollarSign) {
        beforeClose = self.previous;
        sequenceToken = effects.enter("mathTextSequence");
        sizeClose = 0;
        return sequenceClose(code);
      }
      if (code === codes.space) {
        effects.enter("space");
        effects.consume(code);
        effects.exit("space");
        return between;
      }
      if (markdownLineEnding(code)) {
        effects.enter(types.lineEnding);
        effects.consume(code);
        effects.exit(types.lineEnding);
        return between;
      }
      effects.enter("mathTextData");
      return data(code);
    }

    function data(code: Code): State | undefined {
      if (
        code === codes.eof ||
        code === codes.space ||
        code === codes.dollarSign ||
        markdownLineEnding(code)
      ) {
        effects.exit("mathTextData");
        return between(code);
      }
      effects.consume(code);
      return data;
    }

    function sequenceClose(code: Code): State | undefined {
      if (code === codes.dollarSign) {
        effects.consume(code);
        sizeClose += 1;
        return sequenceClose;
      }
      if (sizeClose === sizeOpen && !whitespace(beforeClose) && !digit(code)) {
        effects.exit("mathTextSequence");
        effects.exit("mathText");
        return ok(code);
      }
      if (sizeClose === 1 && code !== codes.eof && !whitespace(code)) {
        return nok(code);
      }
      sequenceToken.type = "mathTextData";
      return data(code);
    }
  },
};

const safeSingleDollarExtension = {
  text: { [codes.dollarSign]: safeSingleDollar },
};

/** Register safe single-dollar syntax beside remark-math's double-dollar syntax. */
export function remarkSafeSingleDollar(this: Processor) {
  const data = this.data() as { micromarkExtensions?: unknown[] };
  const extensions = data.micromarkExtensions ?? [];
  data.micromarkExtensions = extensions;
  extensions.push(safeSingleDollarExtension);
}

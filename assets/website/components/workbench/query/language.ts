import { StreamLanguage } from "@codemirror/language";
import { veinPipe } from "../pipe-language.mjs";

export const language = StreamLanguage.define(veinPipe);

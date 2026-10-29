const BASE_PATH = process.env.NEXT_PUBLIC_BASE_PATH || "";

export function assetUrl(path) {
  return `${BASE_PATH}/${path.replace(/^\/+/, "")}`;
}

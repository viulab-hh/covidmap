import d3 from '../../../utils/d3-import';
import type {
  PromptPoint,
  UMAPPointStreamData,
  LoaderWorkerMessage
} from '../../../types/embedding-types';
import { timeit } from '../../../utils/utils';
import { config } from '../../../config/config';

const DEBUG = config.debug;
const POINT_THRESHOLD = 1000;

let pendingDataPoints: PromptPoint[] = [];
let loadedPointCount = 0;
let sentPointCount = 0;

let lastDrawnPoints: PromptPoint[] | null = null;

/**
 * Handle message events from the main thread
 * @param e Message event
 */
self.onmessage = (e: MessageEvent<LoaderWorkerMessage>) => {
  // Stream point data
  switch (e.data.command) {
    case 'startLoadData': {
      console.log('Worker: start streaming data');
      timeit('Stream data', true);

      const url = e.data.payload.url;
      startLoadData(url);
      break;
    }

    default: {
      console.error('Worker: unknown message', e.data.command);
      break;
    }
  }
};

/**
 * Start loading the large UMAP data
 * @param url URL to the NDJSON file
 */
const startLoadData = async (url: string) => {
  pendingDataPoints = [];
  loadedPointCount = 0;
  sentPointCount = 0;
  lastDrawnPoints = null;

  try {
    const response = await fetch(url);

    if (!response.ok) {
      reportLoadError(`Failed to load data: ${response.status} ${response.statusText}`);
      return;
    }

    if (!response.body) {
      await loadDataFromText(response);
      return;
    }

    await loadDataFromStream(response.body);
  } catch (error) {
    try {
      const response = await fetch(url);
      if (!response.ok) {
        reportLoadError(
          `Failed to load data fallback: ${response.status} ${response.statusText}`
        );
        return;
      }
      await loadDataFromText(response);
    } catch (fallbackError) {
      reportLoadError(getErrorMessage(fallbackError || error));
    }
  }
};

const loadDataFromStream = async (body: ReadableStream<Uint8Array>) => {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const result = await reader.read();
    if (result.done) break;

    buffer += decoder.decode(result.value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      processLine(line);
    }
  }

  buffer += decoder.decode();
  processLine(buffer);
  timeit('Stream data', DEBUG);
  pointStreamFinished();
};

const loadDataFromText = async (response: Response) => {
  const text = await response.text();
  for (const line of text.split('\n')) {
    processLine(line);
  }
  timeit('Stream data', DEBUG);
  pointStreamFinished();
};

const processLine = (line: string) => {
  if (line.trim() === '') return;
  processPointStream(JSON.parse(line) as UMAPPointStreamData);
};

const getErrorMessage = (error: unknown) =>
  error instanceof Error ? error.message : String(error);

const reportLoadError = (message: string) => {
  console.error(message);
  const result: LoaderWorkerMessage = {
    command: 'loadDataError',
    payload: { message }
  };
  postMessage(result);
};

/**
 * Process one data point
 * @param point Loaded data point
 */
const processPointStream = (point: UMAPPointStreamData) => {
  const promptPoint: PromptPoint = {
    x: point[0],
    y: point[1],
    prompt: point[2],
    id: loadedPointCount
  };

  if (point.length > 3) {
    promptPoint.time = point[3]!;
  }

  if (point.length > 4) {
    promptPoint.groupID = point[4]!;
  }

  pendingDataPoints.push(promptPoint);
  loadedPointCount += 1;

  // Notify the main thread if we have load enough data
  if (pendingDataPoints.length >= POINT_THRESHOLD) {
    const result: LoaderWorkerMessage = {
      command: 'transferLoadData',
      payload: {
        isFirstBatch: lastDrawnPoints === null,
        isLastBatch: false,
        points: pendingDataPoints,
        loadedPointCount
      }
    };
    postMessage(result);

    sentPointCount += pendingDataPoints.length;
    lastDrawnPoints = pendingDataPoints.slice();
    pendingDataPoints = [];
  }
};

/**
 * Construct tree and notify the main thread when finish reading all data
 */
const pointStreamFinished = () => {
  // Send any left over points

  const result: LoaderWorkerMessage = {
    command: 'transferLoadData',
    payload: {
      isFirstBatch: lastDrawnPoints === null,
      isLastBatch: true,
      points: pendingDataPoints,
      loadedPointCount
    }
  };
  postMessage(result);

  sentPointCount += pendingDataPoints.length;
  lastDrawnPoints = pendingDataPoints.slice();
  pendingDataPoints = [];
};

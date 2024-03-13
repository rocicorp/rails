export type Request = PullMsg;

type PullMsg = {
  id: number;
  type: 'pull';
};

export type Reply = {
  replyingTo: number;
  type: 'pullResponse';
};

let messageID = 0;

export function nextMessageID() {
  return messageID++;
}

/**
 * PullMessage is sent by leaves up to sources to tell them to send
 * historical data.
 *
 * In the future, pull messages will gather up hoistable
 * expressions and send them to the source to be evaluated.
 *
 * E.g., if there is a filter against the primary key. The source
 * can use that information to restrict the rows it returns.
 */
export function createPullMessage(): Request {
  return {
    id: nextMessageID(),
    type: 'pull',
  };
}

export function createPullResponseMessage(pullMsg: PullMsg): Reply {
  return {
    replyingTo: pullMsg.id,
    type: 'pullResponse',
  };
}

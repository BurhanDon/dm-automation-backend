const WebhookEvent = require('../models/WebhookEvent');
const InstagramAccount = require('../models/InstagramAccount');
const AutomationFlow = require('../models/AutomationFlow');
const Conversation = require('../models/Conversation');
const dmSendQueue = require('./dmQueue');

const parseWebhookPayload = (payload) => {
  if (!payload.entry || !payload.entry[0]) {
    throw new Error('Invalid webhook payload structure');
  }

  const entry = payload.entry[0];
  const messaging = entry.messaging || [];

  return messaging.map((msg) => ({
    senderInstagramId: msg.sender?.id,
    recipientInstagramId: msg.recipient?.id,
    message: msg.message?.text,
    timestamp: msg.timestamp,
    eventType: msg.postback ? 'postback' : 'message',
  }));
};

const processWebhookEvent = async (payload) => {
  try {
    const events = parseWebhookPayload(payload);

    for (const event of events) {
      const account = await InstagramAccount.findOne({
        instagramUserId: event.recipientInstagramId,
      });

      if (!account) {
        console.warn(`No account found for Instagram ID: ${event.recipientInstagramId}`);
        continue;
      }

      const webhookEvent = new WebhookEvent({
        instagramAccountId: account._id,
        eventType: 'direct_message',
        rawPayload: event,
        processed: false,
      });

      await webhookEvent.save();

      const flow = await findMatchingFlow(account._id, event.message);

      if (flow) {
        let conversation = await Conversation.findOne({
          instagramAccountId: account._id,
          recipientInstagramId: event.senderInstagramId,
        });

        if (!conversation) {
          conversation = new Conversation({
            instagramAccountId: account._id,
            recipientInstagramId: event.senderInstagramId,
            activeFlowId: flow._id,
            currentStepIndex: 0,
            windowOpensAt: new Date(),
            windowClosesAt: new Date(Date.now() + 24 * 60 * 60 * 1000),
            status: 'active',
          });

          await conversation.save();
        }

        await dmSendQueue.add(
          'send-dm',
          {
            conversationId: conversation._id.toString(),
            flowId: flow._id.toString(),
            instagramAccountId: account._id.toString(),
            recipientInstagramId: event.senderInstagramId,
            accountAccessToken: account.accessToken,
          },
          {
            attempts: 3,
            backoff: {
              type: 'exponential',
              delay: 2000,
            },
            removeOnComplete: true,
            priority: 10,
            delay: 0,
          }
        );

        webhookEvent.processed = true;
        webhookEvent.processedAt = new Date();
        await webhookEvent.save();
      }
    }
  } catch (error) {
    console.error('Webhook processing error:', error.message);
    throw error;
  }
};

const findMatchingFlow = async (instagramAccountId, messageText) => {
  const flows = await AutomationFlow.find({
    instagramAccountId,
    isActive: true,
    triggerType: 'keyword_dm',
  });

  for (const flow of flows) {
    const { keyword, matchType } = flow.triggerConfig;

    if (!keyword || !messageText) continue;

    const matches =
      matchType === 'exact'
        ? messageText.toLowerCase() === keyword.toLowerCase()
        : messageText.toLowerCase().includes(keyword.toLowerCase());

    if (matches) {
      return flow;
    }
  }

  return null;
};

module.exports = {
  processWebhookEvent,
  dmSendQueue,
  parseWebhookPayload,
};

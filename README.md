# DM Automation Backend

Production-ready Node.js backend for Instagram DM Automation SaaS platform.

## Features

- 🔐 **JWT Authentication** with access/refresh tokens and httpOnly cookies
- 📱 **Instagram OAuth Integration** with Meta Graph API v21.0
- 🤖 **AI Reply System** using OpenAI GPT-4o-mini
- 🔔 **Webhook Signature Verification** with HMAC-SHA256
- 🎯 **Smart Flow Execution** with 24-hour messaging window support
- 💾 **Encryption at Rest** for Instagram access tokens (AES-256-GCM)
- ⚡ **Memcached-backed Queue Locking** for reliable DM delivery dedupe
- 📊 **Rate Limiting** with per-account limits (200 DMs/hour)
- 🔍 **Comprehensive Logging** and audit trail
- ✅ **Input Validation** using express-validator

## Tech Stack

- **Runtime**: Node.js (CommonJS)
- **Framework**: Express.js
- **Database**: MongoDB + Mongoose
- **Cache/Queue Locking**: Memcached + in-process queue adapter
- **Authentication**: JWT + bcrypt
- **Encryption**: AES-256-GCM
- **External APIs**: Meta Graph API v21.0, OpenAI Chat Completions
- **Testing**: Jest
- **Security**: Helmet, CORS, express-rate-limit

## Quick Start

### Prerequisites

- Node.js 18+
- MongoDB 5.0+
- Memcached 1.6+
- Meta Developer Account (for Instagram OAuth)
- OpenAI API Key

### Installation

1. Clone the repository
```bash
cd backend
npm install
```

2. Create `.env` file (copy from `.env.example`)
```bash
cp .env.example .env
```

3. Configure environment variables:
```env
PORT=3001
NODE_ENV=development

# MongoDB
MONGODB_URI=mongodb://localhost:27017/dm-automation

# Memcached
MEMCACHED_SERVERS=127.0.0.1:11211
MEMCACHED_TTL_SECONDS=300

# JWT
JWT_ACCESS_SECRET=your-access-secret-key
JWT_REFRESH_SECRET=your-refresh-secret-key
JWT_ACCESS_EXPIRY=15m
JWT_REFRESH_EXPIRY=7d

# Meta API
META_APP_ID=your-meta-app-id
META_APP_SECRET=your-meta-app-secret
META_WEBHOOK_VERIFY_TOKEN=your-webhook-verify-token

# Encryption
ENCRYPTION_KEY=your-32-byte-hex-encryption-key

# OpenAI
OPENAI_API_KEY=your-openai-api-key

# Client
CLIENT_URL=http://localhost:3000
```

4. Start the server:
```bash
npm run dev
```

Server will be available at `http://localhost:3001`

### Running Services

**Main Server**:
```bash
npm run dev
```

**Job Processor** (in separate terminal):
```bash
npm run jobs
```

## API Endpoints

### Authentication
- `POST /api/auth/register` - Register new user
- `POST /api/auth/login` - Login user
- `POST /api/auth/refresh` - Refresh access token
- `POST /api/auth/logout` - Logout user

### Instagram Account
- `GET /api/instagram/connect` - Get OAuth authorization URL
- `GET /api/instagram/callback` - OAuth callback handler
- `DELETE /api/instagram/disconnect/:accountId` - Disconnect account

### Automation Flows
- `GET /api/flows` - List user's flows
- `POST /api/flows` - Create new flow
- `GET /api/flows/:id` - Get flow details
- `PUT /api/flows/:id` - Update flow
- `DELETE /api/flows/:id` - Delete flow
- `PATCH /api/flows/:id/toggle` - Toggle flow active status

### Webhooks
- `GET /api/webhooks/instagram` - Verify webhook subscription
- `POST /api/webhooks/instagram` - Receive Instagram webhooks

### Lead Capture
- `GET /api/leads` - List leads (paginated)
- `GET /api/leads/:id` - Get lead details
- `DELETE /api/leads/:id` - Delete lead

### Dashboard
- `GET /api/dashboard/stats` - Get dashboard statistics

## Project Structure

```
backend/
├── src/
│   ├── config/          # Configuration files (env, database, memcache)
│   ├── controllers/     # Route handlers
│   ├── middleware/      # Express middleware
│   ├── models/          # Mongoose schemas
│   ├── routes/          # API route definitions
│   ├── services/        # Business logic
│   ├── utils/           # Utility functions (encryption, tokens, etc)
│   ├── jobs/            # Worker bootstrap scripts
│   ├── tests/           # Test files
│   └── server.js        # Express app initialization
├── package.json
├── jest.config.js
└── .env.example
```

## Testing

```bash
npm test
```

## Deployment

### Production Environment Variables

Ensure these are set in your production environment:

```env
NODE_ENV=production
PORT=3001
MONGODB_URI=[production-mongodb-uri]
MEMCACHED_SERVERS=[production-memcached-host:11211]
MEMCACHED_TTL_SECONDS=300
JWT_ACCESS_SECRET=[strong-random-string]
JWT_REFRESH_SECRET=[strong-random-string]
ENCRYPTION_KEY=[32-byte-hex-key]
META_APP_ID=[your-meta-app-id]
META_APP_SECRET=[your-meta-app-secret]
META_WEBHOOK_VERIFY_TOKEN=[your-webhook-verify-token]
OPENAI_API_KEY=[your-openai-api-key]
CLIENT_URL=[production-frontend-url]
```

### Docker (Optional)

```bash
docker build -t dm-automation-backend .
docker run -p 3001:3001 --env-file .env dm-automation-backend
```

## Error Handling

All errors are returned in consistent format:

```json
{
  "success": false,
  "message": "Error description",
  "code": "ERROR_CODE",
  "errors": []  // Validation errors if applicable
}
```

## Rate Limiting

- **Auth Routes**: 10 requests per 15 minutes per IP
- **General API**: 100 requests per minute per IP
- **DM Sending**: 200 DMs per hour per Instagram account

## Security

- HTTPS enforced in production
- CORS configured with whitelist
- Helmet headers applied
- Rate limiting on authentication
- HMAC-SHA256 webhook verification
- AES-256-GCM encryption at rest
- bcrypt password hashing (salt rounds: 10)
- JWT token expiry implemented

## Support

For issues or questions, create an issue in the repository.

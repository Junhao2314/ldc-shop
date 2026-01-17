import { sqliteTable, text, integer } from 'drizzle-orm/sqlite-core';

// Products
export const products = sqliteTable('products', {
    id: text('id').primaryKey(),
    name: text('name').notNull(),
    description: text('description'),
    price: text('price').notNull(), // SQLite doesn't have decimal, use text for precision
    compareAtPrice: text('compare_at_price'),
    category: text('category'),
    image: text('image'),
    isHot: integer('is_hot', { mode: 'boolean' }).default(false),
    isActive: integer('is_active', { mode: 'boolean' }).default(true),
    isShared: integer('is_shared', { mode: 'boolean' }).default(false),
    sortOrder: integer('sort_order').default(0),
    purchaseLimit: integer('purchase_limit'),
    purchaseWarning: text('purchase_warning'), // Optional warning message shown before purchase
    createdAt: integer('created_at', { mode: 'timestamp' }).$defaultFn(() => new Date()), // Use integer timestamp
});

// Cards (Stock)
export const cards = sqliteTable('cards', {
    id: integer('id', { mode: 'number' }).primaryKey({ autoIncrement: true }),
    productId: text('product_id').notNull().references(() => products.id, { onDelete: 'cascade' }),
    cardKey: text('card_key').notNull(),
    isUsed: integer('is_used', { mode: 'boolean' }).default(false),
    reservedOrderId: text('reserved_order_id'),
    reservedAt: integer('reserved_at', { mode: 'timestamp' }),
    usedAt: integer('used_at', { mode: 'timestamp' }),
    createdAt: integer('created_at', { mode: 'timestamp' }).$defaultFn(() => new Date()),
});

// Orders
export const orders = sqliteTable('orders', {
    orderId: text('order_id').primaryKey(),
    productId: text('product_id').notNull(),
    productName: text('product_name').notNull(),
    amount: text('amount').notNull(),
    email: text('email'),
    status: text('status').default('pending'), // pending, paid, delivered, failed, refunded
    tradeNo: text('trade_no'),
    cardKey: text('card_key'),
    paidAt: integer('paid_at', { mode: 'timestamp' }),
    deliveredAt: integer('delivered_at', { mode: 'timestamp' }),
    userId: text('user_id'),
    username: text('username'),
    payee: text('payee'),
    pointsUsed: integer('points_used').default(0),
    quantity: integer('quantity').default(1).notNull(),
    currentPaymentId: text('current_payment_id'),
    createdAt: integer('created_at', { mode: 'timestamp' }).$defaultFn(() => new Date()),
});

// Logged-in users (for visitor counts)
export const loginUsers = sqliteTable('login_users', {
    userId: text('user_id').primaryKey(),
    username: text('username'),
    points: integer('points').default(0).notNull(),
    isBlocked: integer('is_blocked', { mode: 'boolean' }).default(false),
    createdAt: integer('created_at', { mode: 'timestamp' }).$defaultFn(() => new Date()),
    lastLoginAt: integer('last_login_at', { mode: 'timestamp' }).$defaultFn(() => new Date()),
});

// Daily Check-ins
export const dailyCheckins = sqliteTable('daily_checkins_v2', {
    id: integer('id', { mode: 'number' }).primaryKey({ autoIncrement: true }),
    userId: text('user_id').notNull().references(() => loginUsers.userId, { onDelete: 'cascade' }),
    createdAt: integer('created_at', { mode: 'timestamp' }).$defaultFn(() => new Date()),
});
// Note: Unique index logic for 'userDateUnique' needs Drizzle SQLite specific syntax or raw SQL if not supported directly in schema builder yet, 
// but for now relying on application application level check or standard unique() if supported.
// Actually Drizzle SQLite supports unique(). 
// But complex index on function date(createdAt) might need pure SQL or separate index definition.
// For D1/SQLite, we can't easily index on function in Drizzle schema builder directly easily without `generated always as`.
// We will handle the "check in once per day" logic in application code query or redundant column if needed.
// However, checking the migration logic later is cleaner.

// Settings
export const settings = sqliteTable('settings', {
    key: text('key').primaryKey(),
    value: text('value'),
    updatedAt: integer('updated_at', { mode: 'timestamp' }).$defaultFn(() => new Date()),
});

// Reviews
export const reviews = sqliteTable('reviews', {
    id: integer('id', { mode: 'number' }).primaryKey({ autoIncrement: true }),
    productId: text('product_id').notNull().references(() => products.id, { onDelete: 'cascade' }),
    orderId: text('order_id').notNull(),
    userId: text('user_id').notNull(),
    username: text('username').notNull(),
    rating: integer('rating').notNull(), // 1-5 stars
    comment: text('comment'),
    createdAt: integer('created_at', { mode: 'timestamp' }).$defaultFn(() => new Date()),
});

// Categories
export const categories = sqliteTable('categories', {
    id: integer('id', { mode: 'number' }).primaryKey({ autoIncrement: true }),
    name: text('name').notNull(),
    icon: text('icon'),
    sortOrder: integer('sort_order').default(0),
    createdAt: integer('created_at', { mode: 'timestamp' }).$defaultFn(() => new Date()),
    updatedAt: integer('updated_at', { mode: 'timestamp' }).$defaultFn(() => new Date()),
});

// Refund requests
export const refundRequests = sqliteTable('refund_requests', {
    id: integer('id', { mode: 'number' }).primaryKey({ autoIncrement: true }),
    orderId: text('order_id').notNull(),
    userId: text('user_id'),
    username: text('username'),
    reason: text('reason'),
    status: text('status').default('pending'),
    adminUsername: text('admin_username'),
    adminNote: text('admin_note'),
    createdAt: integer('created_at', { mode: 'timestamp' }).$defaultFn(() => new Date()),
    updatedAt: integer('updated_at', { mode: 'timestamp' }).$defaultFn(() => new Date()),
    processedAt: integer('processed_at', { mode: 'timestamp' }),
});

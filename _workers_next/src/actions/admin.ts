'use server'

import { auth } from "@/lib/auth"
import { db } from "@/lib/db"
import { products, cards, reviews, categories } from "@/lib/db/schema"
import { eq, sql } from "drizzle-orm"
import { revalidatePath } from "next/cache"
import { setSetting } from "@/lib/db/queries"

// Check Admin Helper
// Check Admin Helper
export async function checkAdmin() {
    const session = await auth()
    const user = session?.user
    const adminUsers = process.env.ADMIN_USERS?.toLowerCase().split(',') || []
    if (!user || !user.username || !adminUsers.includes(user.username.toLowerCase())) {
        throw new Error("Unauthorized")
    }
}

export async function saveProduct(formData: FormData) {
    await checkAdmin()

    const id = formData.get('id') as string || `prod_${Date.now()}`
    const name = formData.get('name') as string
    const description = formData.get('description') as string
    const price = formData.get('price') as string
    const compareAtPrice = (formData.get('compareAtPrice') as string | null) || null
    const category = formData.get('category') as string
    const image = formData.get('image') as string
    const purchaseLimit = formData.get('purchaseLimit') ? parseInt(formData.get('purchaseLimit') as string) : null
    const isHot = formData.get('isHot') === 'on'

    const doSave = async () => {
        // Auto-create category if it doesn't exist
        if (category) {
            await ensureCategoriesTable()
            await db.run(sql`
                INSERT INTO categories (name, updated_at) 
                VALUES (${category}, (unixepoch() * 1000)) 
                ON CONFLICT (name) DO NOTHING
            `)
        }

        await db.insert(products).values({
            id,
            name,
            description,
            price,
            compareAtPrice: compareAtPrice && compareAtPrice !== '0' ? compareAtPrice : null,
            category,
            image,
            purchaseLimit,
            isHot
        }).onConflictDoUpdate({
            target: products.id,
            set: {
                name,
                description,
                price,
                compareAtPrice: compareAtPrice && compareAtPrice !== '0' ? compareAtPrice : null,
                category,
                image,
                purchaseLimit,
                isHot
            }
        })
    }

    try {
        await doSave()
    } catch (error: any) {
        const errorString = JSON.stringify(error)
        if (errorString.includes('42703')) {
            try {
                await db.run(sql.raw(`ALTER TABLE products ADD COLUMN compare_at_price TEXT`));
            } catch { /* duplicate column */ }
            try {
                await db.run(sql.raw(`ALTER TABLE products ADD COLUMN is_hot INTEGER DEFAULT 0`));
            } catch { /* duplicate column */ }
            await doSave()
        } else {
            throw error
        }
    }

    revalidatePath('/admin')
    revalidatePath('/')
}

export async function deleteProduct(id: string) {
    await checkAdmin()
    await db.delete(products).where(eq(products.id, id))
    revalidatePath('/admin')
    revalidatePath('/')
}

export async function toggleProductStatus(id: string, isActive: boolean) {
    await checkAdmin()
    await db.update(products).set({ isActive }).where(eq(products.id, id))
    revalidatePath('/admin')
    revalidatePath('/')
}

export async function reorderProduct(id: string, newOrder: number) {
    await checkAdmin()
    await db.update(products).set({ sortOrder: newOrder }).where(eq(products.id, id))
    revalidatePath('/admin')
    revalidatePath('/')
}

export async function addCards(formData: FormData) {
    await checkAdmin()
    const productId = formData.get('product_id') as string
    const rawCards = formData.get('cards') as string

    const cardList = rawCards
        .split(/[\n,]+/)
        .map(c => c.trim())
        .filter(c => c)

    if (cardList.length === 0) return

    try {
        await db.run(sql`DROP INDEX IF EXISTS cards_product_id_card_key_uq;`)
    } catch {
        // best effort
    }

    // D1 has a limit on SQL variables (around 100 bindings per query)
    // Each card insert uses ~2 variables, so batch in groups of 30 to be safe
    const BATCH_SIZE = 30
    for (let i = 0; i < cardList.length; i += BATCH_SIZE) {
        const batch = cardList.slice(i, i + BATCH_SIZE)
        await db.insert(cards).values(
            batch.map(key => ({
                productId,
                cardKey: key
            }))
        )
    }

    revalidatePath('/admin')
    revalidatePath(`/admin/cards/${productId}`)
    revalidatePath('/')
}

export async function deleteCard(cardId: number) {
    await checkAdmin()

    // Only delete unused cards
    const card = await db.query.cards.findFirst({
        where: eq(cards.id, cardId)
    })

    if (!card) {
        throw new Error("Card not found")
    }

    if (card.isUsed) {
        throw new Error("Cannot delete used card")
    }
    if (card.reservedAt && card.reservedAt > new Date(Date.now() - 60 * 1000)) {
        throw new Error("Cannot delete reserved card")
    }

    await db.delete(cards).where(eq(cards.id, cardId))

    revalidatePath('/admin')
    revalidatePath('/admin/cards')
    revalidatePath('/')
}

export async function saveShopName(rawName: string) {
    await checkAdmin()

    const name = rawName.trim()
    if (!name) {
        throw new Error("Shop name cannot be empty")
    }
    if (name.length > 64) {
        throw new Error("Shop name is too long")
    }

    try {
        await setSetting('shop_name', name)
    } catch (error: any) {
        // If settings table doesn't exist, create it and retry
        if (error.message?.includes('does not exist') ||
            error.code === '42P01' ||
            JSON.stringify(error).includes('42P01')) {
            await db.run(sql`
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at INTEGER DEFAULT (unixepoch() * 1000)
                )
            `)
            await setSetting('shop_name', name)
        } else {
            throw error
        }
    }

    revalidatePath('/')
    revalidatePath('/admin')
}

export async function deleteReview(reviewId: number) {
    await checkAdmin()
    await db.delete(reviews).where(eq(reviews.id, reviewId))
    revalidatePath('/admin/reviews')
}

export async function saveLowStockThreshold(raw: string) {
    await checkAdmin()
    const n = Number.parseInt(String(raw || '').trim(), 10)
    const value = Number.isFinite(n) && n > 0 ? String(n) : '5'
    await setSetting('low_stock_threshold', value)
    revalidatePath('/admin')
}

export async function saveCheckinReward(raw: string) {
    await checkAdmin()
    const n = Number.parseInt(String(raw || '').trim(), 10)
    const value = Number.isFinite(n) && n > 0 ? String(n) : '10'
    await setSetting('checkin_reward', value)
    await setSetting('checkin_reward', value)
    revalidatePath('/admin')
}

export async function saveCheckinEnabled(enabled: boolean) {
    await checkAdmin()
    await setSetting('checkin_enabled', enabled ? 'true' : 'false')
    revalidatePath('/admin')
    revalidatePath('/')
}

async function ensureCategoriesTable() {
    await db.run(sql`
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            icon TEXT,
            sort_order INTEGER DEFAULT 0,
            created_at INTEGER DEFAULT (unixepoch() * 1000),
            updated_at INTEGER DEFAULT (unixepoch() * 1000)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS categories_name_uq ON categories(name);
    `)
}

export async function saveCategory(formData: FormData) {
    await checkAdmin()
    await ensureCategoriesTable()

    const idRaw = formData.get('id') as string | null
    const name = String(formData.get('name') || '').trim()
    const icon = String(formData.get('icon') || '').trim() || null
    const sortOrder = Number.parseInt(String(formData.get('sortOrder') || '0'), 10) || 0
    if (!name) throw new Error("Category name is required")

    if (idRaw) {
        const id = Number.parseInt(idRaw, 10)
        await db.update(categories).set({ name, icon, sortOrder, updatedAt: new Date() }).where(eq(categories.id, id))
    } else {
        await db.insert(categories).values({ name, icon, sortOrder, updatedAt: new Date() })
    }

    revalidatePath('/admin/categories')
    revalidatePath('/')
}

export async function deleteCategory(id: number) {
    await checkAdmin()
    await ensureCategoriesTable()
    await db.delete(categories).where(eq(categories.id, id))
    revalidatePath('/admin/categories')
    revalidatePath('/')
}

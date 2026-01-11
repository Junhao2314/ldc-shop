import { db } from "@/lib/db"
import { orders } from "@/lib/db/schema"
import { desc } from "drizzle-orm"
import { getProducts, getDashboardStats, getSetting, getVisitorCount, getRecentOrders } from "@/lib/db/queries"
import { AdminProductsContent } from "@/components/admin/products-content"

export default async function AdminPage() {
    const [products, stats, shopName, visitorCount, lowStockThreshold, checkinReward, checkinEnabled] = await Promise.all([
        getProducts(),
        getDashboardStats(),
        (async () => {
            try {
                return await getSetting('shop_name')
            } catch {
                return null
            }
        })(),
        (async () => {
            try {
                return await getVisitorCount()
            } catch {
                return 0
            }
        })(),
        (async () => {
            try {
                const v = await getSetting('low_stock_threshold')
                return Number.parseInt(v || '5', 10) || 5
            } catch {
                return 5
            }
        })(),
        // Removed recentOrders query
        (async () => {
            try {
                const v = await getSetting('checkin_reward')
                return Number.parseInt(v || '10', 10) || 10
            } catch {
                return 10
            }
        })(),
        (async () => {
            try {
                const v = await getSetting('checkin_enabled')
                return v !== 'false' // Default to true
            } catch {
                return true
            }
        })(),
    ])

    return (
        <AdminProductsContent
            products={products.map((p: any) => ({
                id: p.id,
                name: p.name,
                price: p.price,
                compareAtPrice: p.compareAtPrice ?? null,
                category: p.category,
                stockCount: p.stock,
                isActive: p.isActive ?? true,
                isHot: p.isHot ?? false,
                sortOrder: p.sortOrder ?? 0
            }))}
            stats={stats}
            shopName={shopName}
            visitorCount={visitorCount}
            lowStockThreshold={lowStockThreshold}
            checkinReward={checkinReward}
            checkinEnabled={checkinEnabled}
        />
    )
}

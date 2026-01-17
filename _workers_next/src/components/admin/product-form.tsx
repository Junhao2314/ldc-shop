'use client'

import { saveProduct } from "@/actions/admin"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { useState } from "react"
import { toast } from "sonner"
import { useRouter } from "next/navigation"
import { useI18n } from "@/lib/i18n/context"

export default function ProductForm({ product, categories = [] }: { product?: any; categories?: Array<{ name: string }> }) {
    const router = useRouter()
    const [loading, setLoading] = useState(false)
    // Only show warning section if purchaseWarning has actual content
    const [showWarning, setShowWarning] = useState(Boolean(product?.purchaseWarning && String(product.purchaseWarning).trim()))
    const { t } = useI18n()

    async function handleSubmit(formData: FormData) {
        setLoading(true)
        try {
            await saveProduct(formData)
            toast.success(t('common.success'))
            router.push('/admin/products')
        } catch (e: any) {
            console.error('Save product error:', e)
            toast.error(e?.message || t('common.error'))
        } finally {
            setLoading(false)
        }
    }

    return (
        <Card className="max-w-2xl mx-auto">
            <CardHeader>
                <CardTitle>{product ? t('admin.productForm.editTitle') : t('admin.productForm.addTitle')}</CardTitle>
            </CardHeader>
            <CardContent>
                <form action={handleSubmit} className="space-y-4">
                    {product && <input type="hidden" name="id" value={product.id} />}

                    <div className="grid gap-2">
                        <Label htmlFor="slug">{t('admin.productForm.slugLabel')}</Label>
                        <div className="flex items-center gap-2">
                            <span className="text-sm text-muted-foreground">/buy/</span>
                            <Input
                                id="slug"
                                name="slug"
                                defaultValue={product?.id || ''}
                                placeholder={t('admin.productForm.slugPlaceholder')}
                                pattern="^[a-zA-Z0-9_-]+$"
                                className="flex-1"
                                disabled={!!product}
                            />
                        </div>
                        <p className="text-xs text-muted-foreground">
                            {product ? t('admin.productForm.slugReadonly') : t('admin.productForm.slugHint')}
                        </p>
                    </div>

                    <div className="grid gap-2">
                        <Label htmlFor="name">{t('admin.productForm.nameLabel')}</Label>
                        <Input id="name" name="name" defaultValue={product?.name} placeholder={t('admin.productForm.namePlaceholder')} required />
                    </div>

                    <div className="grid gap-2">
                        <Label htmlFor="price">{t('admin.productForm.priceLabel')}</Label>
                        <Input id="price" name="price" type="number" step="0.01" defaultValue={product?.price} placeholder={t('admin.productForm.pricePlaceholder')} required />
                    </div>

                    <div className="grid gap-2">
                        <Label htmlFor="compareAtPrice">{t('admin.productForm.compareAtPriceLabel')}</Label>
                        <Input
                            id="compareAtPrice"
                            name="compareAtPrice"
                            type="number"
                            step="0.01"
                            defaultValue={product?.compareAtPrice || ''}
                            placeholder={t('admin.productForm.compareAtPricePlaceholder')}
                        />
                    </div>

                    <div className="grid gap-2">
                        <Label htmlFor="purchaseLimit">{t('admin.productForm.purchaseLimitLabel') || "Purchase Limit (0 or empty for unlimited)"}</Label>
                        <Input id="purchaseLimit" name="purchaseLimit" type="number" defaultValue={product?.purchaseLimit} placeholder={t('admin.productForm.purchaseLimitPlaceholder') || "e.g. 1"} />
                    </div>

                    <div className="grid gap-2">
                        <Label htmlFor="category">{t('admin.productForm.categoryLabel')}</Label>
                        <Input id="category" name="category" list="ldc-category-list" defaultValue={product?.category} placeholder={t('admin.productForm.categoryPlaceholder')} />
                        <datalist id="ldc-category-list">
                            {categories.map(c => (
                                <option key={c.name} value={c.name} />
                            ))}
                        </datalist>
                    </div>

                    <div className="flex items-center space-x-2">
                        <Checkbox
                            id="isShared"
                            name="isShared"
                            defaultChecked={product?.isShared ?? false}
                            className="h-4 w-4 accent-primary"
                        />
                        <div className="flex flex-col">
                            <Label htmlFor="isShared" className="cursor-pointer font-medium">{t('admin.productForm.isSharedLabel')}</Label>
                            <span className="text-xs text-muted-foreground">{t('admin.productForm.isSharedHint')}</span>
                        </div>
                    </div>

                    <div className="flex items-center space-x-2">
                        <Checkbox
                            id="isHot"
                            name="isHot"
                            defaultChecked={!!product?.isHot}
                            className="h-4 w-4 accent-primary"
                        />
                        <Label htmlFor="isHot" className="cursor-pointer">{t('admin.productForm.isHotLabel')}</Label>
                    </div>

                    <div className="space-y-2 p-3 border rounded-md bg-muted/30">
                        <div className="flex items-center gap-2">
                            <input
                                id="showWarning"
                                type="checkbox"
                                checked={showWarning}
                                onChange={(e) => setShowWarning(e.target.checked)}
                                className="h-4 w-4 accent-primary"
                            />
                            <Label htmlFor="showWarning" className="cursor-pointer">{t('admin.productForm.purchaseWarningLabel')}</Label>
                        </div>
                        {showWarning && (
                            <div className="grid gap-2">
                                <textarea
                                    id="purchaseWarning"
                                    name="purchaseWarning"
                                    defaultValue={product?.purchaseWarning || ''}
                                    placeholder={t('admin.productForm.purchaseWarningPlaceholder')}
                                    className="flex min-h-[60px] w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                                />
                                <p className="text-xs text-muted-foreground">{t('admin.productForm.purchaseWarningHint')}</p>
                            </div>
                        )}
                    </div>

                    <div className="grid gap-2">
                        <Label htmlFor="image">{t('admin.productForm.imageLabel')}</Label>
                        <Input id="image" name="image" defaultValue={product?.image} placeholder={t('admin.productForm.imagePlaceholder')} />
                    </div>

                    <div className="grid gap-2">
                        <Label htmlFor="description">{t('admin.productForm.descLabel')}</Label>
                        <textarea
                            id="description"
                            name="description"
                            defaultValue={product?.description}
                            placeholder={t('admin.productForm.descPlaceholder')}
                            className="flex min-h-[80px] w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                        />
                    </div>

                    <div className="pt-4 flex justify-end gap-2">
                        <Button variant="outline" type="button" onClick={() => router.back()}>{t('common.cancel')}</Button>
                        <Button type="submit" disabled={loading}>{loading ? t('admin.productForm.saving') : t('admin.productForm.saveButton')}</Button>
                    </div>
                </form>
            </CardContent>
        </Card>
    )
}

export enum Category {
    HOT_TOPICS,
    TOPICS_PER_COUNTRY,
    GLOBAL_EVENTS
}

export const CategoryType = new Map<number, string>([
    [Category.HOT_TOPICS, 'HOT_TOPICS'],
    [Category.TOPICS_PER_COUNTRY, 'TOPICS_PER_COUNTRY'],
    [Category.GLOBAL_EVENTS, 'GLOBAL_EVENTS']
])
-- ==========================================
-- 大学生网络行为画像系统 - Docker 自动初始化脚本
-- 适用数据库: PostgreSQL
-- 作用: 自动创建核心数据表与索引
-- ==========================================

-- 开启扩展 (如果未来需要 UUID 等高级功能，可以解除下面这行的注释)
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. 创建学生基础特征表 (Tier 1 & Tier 2)
CREATE TABLE IF NOT EXISTS student_metrics (
    student_id VARCHAR(50) PRIMARY KEY,         -- 哈希脱敏后的学号
    gpa NUMERIC(3, 2),                          -- 绩点 (如 3.85)
    failed_courses INTEGER DEFAULT 0,           -- 挂科数
    library_visits_per_month INTEGER DEFAULT 0, -- 月均图书馆次数
    late_return_count INTEGER DEFAULT 0,        -- 宿舍晚归次数
    gaming_traffic_ratio NUMERIC(4, 3),         -- 游戏流量占比 (如 0.154)
    breakfast_frequency NUMERIC(4, 3),          -- 规律早饭频率 (如 0.850)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE student_metrics IS '学生基础特征表(Tier1 & Tier2)';

-- 2. 创建学生文本交互表 (Tier 4)
CREATE TABLE IF NOT EXISTS student_texts (
    text_id SERIAL PRIMARY KEY,                 -- 自增主键
    student_id VARCHAR(50) REFERENCES student_metrics(student_id) ON DELETE CASCADE,
    content TEXT NOT NULL,                      -- 文本内容
    source_platform VARCHAR(50),                -- 数据来源 (如 '校园BBS')
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE student_texts IS '学生文本交互表(Tier4)';

-- 3. 创建学生最终画像表 (算法产出)
-- 使用 JSONB 类型以支持无缝扩展新标签
CREATE TABLE IF NOT EXISTS student_profiles (
    student_id VARCHAR(50) PRIMARY KEY REFERENCES student_metrics(student_id) ON DELETE CASCADE,
    basic_tags JSONB DEFAULT '[]'::jsonb,       -- 事实标签
    behavior_tags JSONB DEFAULT '[]'::jsonb,    -- 行为标签
    cognitive_tags JSONB DEFAULT '[]'::jsonb,   -- 认知情绪标签
    radar_scores JSONB DEFAULT '{}'::jsonb,     -- 雷达图得分
    intervention_action TEXT,                   -- 干预建议
    last_computed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE student_profiles IS '学生最终画像表(Layer2产出)';

-- ==========================================
-- 4. 创建 GIN 索引，优化 JSONB 标签的查询速度
-- ==========================================
CREATE INDEX IF NOT EXISTS idx_profiles_behavior_tags ON student_profiles USING GIN (behavior_tags);
CREATE INDEX IF NOT EXISTS idx_profiles_cognitive_tags ON student_profiles USING GIN (cognitive_tags);

-- 脚本执行完毕提示
DO $$
BEGIN
    RAISE NOTICE '✅ 所有的思政画像数据表与索引已成功创建！';
END $$;

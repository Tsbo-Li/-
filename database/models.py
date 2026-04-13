from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()


class StudentMetric(Base):
    __tablename__ = "student_metrics"
    __table_args__ = {"comment": "学生基础特征表(Tier1 & Tier2)"}

    student_id = Column(String(50), primary_key=True, comment="哈希脱敏后的学号")
    gpa = Column(Numeric(3, 2), comment="绩点")
    failed_courses = Column(Integer, default=0, comment="挂科数")
    library_visits_per_month = Column(Integer, default=0, comment="月均图书馆次数")
    late_return_count = Column(Integer, default=0, comment="宿舍晚归次数")
    gaming_traffic_ratio = Column(Numeric(4, 3), comment="游戏流量占比")
    breakfast_frequency = Column(Numeric(4, 3), comment="规律早饭频率")

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    texts = relationship("StudentText", back_populates="student", cascade="all, delete-orphan")
    profile = relationship("StudentProfile", back_populates="student", uselist=False, cascade="all, delete-orphan")


class StudentText(Base):
    __tablename__ = "student_texts"
    __table_args__ = {"comment": "学生文本交互表(Tier4)"}

    text_id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String(50), ForeignKey("student_metrics.student_id", ondelete="CASCADE"))
    content = Column(Text, nullable=False, comment="文本内容")
    source_platform = Column(String(50), comment="数据来源")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    student = relationship("StudentMetric", back_populates="texts")


class StudentProfile(Base):
    __tablename__ = "student_profiles"
    __table_args__ = {"comment": "学生最终画像表(Layer2产出)"}

    student_id = Column(String(50), ForeignKey("student_metrics.student_id", ondelete="CASCADE"), primary_key=True)
    basic_tags = Column(JSONB, server_default=text("'[]'::jsonb"), comment="事实标签列表")
    behavior_tags = Column(JSONB, server_default=text("'[]'::jsonb"), comment="行为标签列表")
    cognitive_tags = Column(JSONB, server_default=text("'[]'::jsonb"), comment="认知情绪标签列表")
    radar_scores = Column(JSONB, server_default=text("'{}'::jsonb"), comment="供前端渲染的雷达图得分")
    intervention_action = Column(Text, comment="干预建议")
    last_computed_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    student = relationship("StudentMetric", back_populates="profile")

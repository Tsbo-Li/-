from fastapi import FastAPI
from db_models.schemas import StudentProfile

app = FastAPI(title="高校精准思政画像 API")

@app.get("/api/profile/{student_id}", response_model=StudentProfile)
async def get_student_profile(student_id: str):
    # TODO: 从 PG 数据库实际查询
    # 此处返回 mock 数据
    return StudentProfile(
        student_id=student_id,
        basic_tags=["文科类"],
        behavior_tags=["高频活跃"],
        cognitive_tags=["考研焦虑"],
        update_timestamp="2023-11-20 12:00:00"
    )

# 启动命令: uvicorn api_server.main:app --reload

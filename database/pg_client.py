from .profile_repository import ProfileRepository


class PostgresClient(ProfileRepository):
    """
    兼容旧命名，内部复用新的 ProfileRepository。
    """

    pass

import enum
from pydantic import BaseModel, EmailStr 
from datetime import datetime

class EventKind(enum.Enum):
    view = "view"
    like = "like"
    share = "share"

class Event(BaseModel):
    id: int
    user_id: str | None
    post_id: str
    kind: EventKind
    timestamp: datetime

class Post(BaseModel):
    title: str
    description: str
    content: str
    userId: int
    created_at: datetime
    likes: int
    views: int
    class Config:
        from_attributes: bool = True

class PostViewResponse(BaseModel):
    title: str
    description: str
    creatorId: int
    created_at: datetime
    likes: int
    views: int
    viewed_at: datetime
    class Config:
        from_attributes: bool = True

class UserPublicProfile(BaseModel):
    username: str
    totalPosts: int
    totalViews: int
    categories: list[str]
    class Config:
        from_attributes: bool = True

class UserSearch(BaseModel):
    query: str
    minPosts: int
    minViews: int #estaremos implementando alguna forma de seguir usuarios?
    minLikes: int

class PostRead(BaseModel):
    userId: int
    postId: int
    readAt: datetime

class HistorySearchRequest(BaseModel):
    userId: int
    minDate: datetime
    maxDate: datetime
    min: int
    max: int


class SearchRequest(BaseModel):
    query: str = None
    category: str = None #esto sería un enum pero prefiero no ponerlo todavía como tal
    username: str = None
    minLikes: int = 0
    minViews: int = 0
    orderBy: str = "views" #esto sería también un enum pero prefiero no ponerlo todavía
    min: int = 1
    max: int = 20

class User(BaseModel):
    userId: int
    username: str
    password: str
    email: EmailStr

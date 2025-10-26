from pydantic import BaseModel
from datetime import datetime

class TopTagsResponse(BaseModel):
    tag: str

class TopArticlesResponse(BaseModel):
    article_id: str
    author_id: str
    username: str
    title: str

class ActiveUsersResponse(BaseModel):
    user_id: str
    email: str
    username: str
    expiration_time: datetime

class UserCount(BaseModel):
    user_count: int

class TopUsersResponse(BaseModel):
    user_id: str
    email: str
    username: str
    views_received: int
    likes_received: int
    shares_received: int
    comments_received: int


















"""
class PostSummary(BaseModel):
    post_id: str
    title: str
    author_id: str
    author_username: str
    views: int
    likes: int
    shares: int
    comments: int

class CategorySummary(BaseModel):
    category: str
    category_views: int
    category_likes: int
    category_shares: int
    category_comments: int

class UserArticlesSummary(BaseModel):
    user_id: str
    username: str
    views_received: int
    likes_received: int
    shares_received: int
    comments_received: int

class UserActivitySummary(BaseModel):
    user_id: str
    username: str
    views_made: int
    likes_made: int
    shares_made: int
    comments_made: int
"""

"""
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
"""

#ifndef _SLS_LOG_PB_PARSER_H_
#define _SLS_LOG_PB_PARSER_H_

#include <vector>
#include <ratio>

namespace aliyun {
namespace log { 

#define IF_NULL_RETURN_FALSE(ptr)  { if (ptr == NULL) { return false;} }
#define IF_FAIL_RETURN_FALSE(exp)  { if ((exp) == false) { return false;}}
#define IF_CONDITION_RETURN_FALSE(condition) { if (condition) { return false;}}

inline static const char* GetVarint32Ptr(const char* p,const char* limit, uint32_t* v)
{
#define BITS_VALUE(ptr, offset)   (((uint32_t)(*(ptr))) << offset)
#define BITS_VALUE_WITH_MASK(ptr, offset)   (((uint32_t)(((uint8_t)(*(ptr))) ^ MASK)) << offset)
    if (p == NULL) 
    {
        return NULL;
    }
    const static uint8_t MASK = 128;
    if (p < limit && ((*p) & MASK) == 0) 
    {
        *v = *p;
        return p + 1;
    }
    else if (p + 1 < limit && !((*(p + 1)) & MASK))
    {
        *v = BITS_VALUE_WITH_MASK(p, 0) |  BITS_VALUE(p + 1 , 7);
        return p + 2;
    }
    else if (p + 2 < limit && !((*(p + 2)) & MASK))
    {
        *v = BITS_VALUE_WITH_MASK(p, 0) |  BITS_VALUE_WITH_MASK(p + 1 , 7) | BITS_VALUE(p + 2 , 14);
        return p + 3;
    }
    else if (p + 3 < limit && !((*(p + 3)) & MASK))
    {
        *v = BITS_VALUE_WITH_MASK(p, 0) |  BITS_VALUE_WITH_MASK(p + 1 , 7) | BITS_VALUE_WITH_MASK(p + 2 , 14) | BITS_VALUE( p + 3 , 21);
        return p + 4;
    }
    else if (p + 4 < limit && !((*(p + 4)) & MASK))
    {
        *v = BITS_VALUE_WITH_MASK(p, 0) |  BITS_VALUE_WITH_MASK(p + 1 , 7) | BITS_VALUE_WITH_MASK(p + 2 , 14) | BITS_VALUE_WITH_MASK( p + 3 , 21) | BITS_VALUE(p + 4 , 28);
        return p + 5;
    }
    *v = 0;
    return NULL;

#undef BITS_VALUE
#undef BITS_VALUE_WITH_MASK
}

class SlsPbValidator
{
    static bool IsValidPair(const char* ptr, const char* ptr_end)
    {
        // only two field exist , utf-8 is not checked
        //  string key = 1;
        //  string value = 2;
        for ( uint32_t i = 1 ; i <=  2; i++)
        {
            uint32_t head = 0;
            ptr = GetVarint32Ptr(ptr, ptr_end, &head);
            IF_NULL_RETURN_FALSE(ptr);
            uint32_t mode = head & 0x7;
            uint32_t index = head >> 3;
            IF_CONDITION_RETURN_FALSE(index != i || mode != 2);
            uint32_t len = 0;
            ptr = GetVarint32Ptr(ptr, ptr_end, &len);
            IF_CONDITION_RETURN_FALSE(ptr == NULL || ptr + len > ptr_end);
            ptr += len;
        }
        return ptr == ptr_end;
    }

    static bool IsValidTag(const char* ptr, const char* ptr_end)
    {
        return IsValidPair(ptr, ptr_end);
    }

    static bool IsValidLog(const char* ptr, const char* ptr_end)
    {
        bool has_time = false;
        while (ptr < ptr_end)
        {
            uint32_t head = 0;
            ptr = GetVarint32Ptr(ptr, ptr_end, &head);
            IF_NULL_RETURN_FALSE(ptr);
            uint32_t mode = head & 0x7;
            uint32_t index = head >> 3;
            if (index == 1)
            {
                IF_CONDITION_RETURN_FALSE(mode != 0 || has_time);
                has_time = true;
                uint32_t data_time;
                ptr = GetVarint32Ptr(ptr, ptr_end, &data_time);
                IF_NULL_RETURN_FALSE(ptr);
            }
            else if (index == 2)
            {
                IF_CONDITION_RETURN_FALSE(mode != 2);
                uint32_t len = 0;
                ptr = GetVarint32Ptr(ptr, ptr_end, &len);
                IF_CONDITION_RETURN_FALSE(ptr == NULL || ptr + len > ptr_end);
                IF_FAIL_RETURN_FALSE(IsValidPair(ptr, ptr + len));
                ptr += len;
            }
            else
            {
                return false;
            }
        }
        return has_time && ptr == ptr_end;
    }

public : 
    static bool IsValidLogGroup(const char* ptr, int32_t len, std::string& topic_value)
    {
        const char* ptr_end = ptr + len;
        while (ptr < ptr_end)
        {
            uint32_t head = 0;
            ptr = GetVarint32Ptr(ptr, ptr_end, &head);
            IF_NULL_RETURN_FALSE(ptr);
            uint32_t mode = head & 0x7;
            uint32_t index = head >> 3;
            if ( index >= 1 && index <= 6)
            {
                // all the mode for index [1, 6] should be 2
                IF_CONDITION_RETURN_FALSE(mode !=2);
                uint32_t len = 0;
                ptr = GetVarint32Ptr(ptr, ptr_end,  &len);
                IF_CONDITION_RETURN_FALSE (ptr == NULL || ptr + len > ptr_end)
                if (index == 1)
                {
                    // check log
                    IF_FAIL_RETURN_FALSE(IsValidLog(ptr, ptr + len));
                }
                else if (index == 3)
                {
                    topic_value = std::string(ptr, len);
                }
                else if ( index == 6)
                {
                    // check tag;
                    IF_FAIL_RETURN_FALSE(IsValidTag(ptr, ptr + len));
                }
                ptr += len;
            }
            else
            {
                return false;
            }
        }
        return true;
    }
};


struct SlsPbOffsetLen
{
    int32_t mOffset;
    int32_t mLen;
    SlsPbOffsetLen() :
        mOffset(0),
        mLen(0)
    {
    }
    SlsPbOffsetLen(int32_t offset, int32_t len) :
        mOffset(offset),
        mLen(len)
    {
    }
};

struct SlsStringPiece
{
    const char* mPtr;
    int32_t mLen;
    SlsStringPiece(const char* ptr, int32_t len) : mPtr(ptr), mLen(len)
    {
    }
    SlsStringPiece() : mPtr(NULL), mLen(0)
    {
    }

    std::string ToString() const
    {
        if (mPtr == NULL)
        {
            return std::string();
        }
        return std::string(mPtr, mLen);
    }

public:
    bool operator<(const SlsStringPiece & ssp2) const
    {
        if (mLen < ssp2.mLen) {
            return true;
        } else{
            return false;
        }
    }
};


static bool ParseKeyValuePb(const char* ptr, const char* ptr_end, SlsStringPiece& key, SlsStringPiece& value)
{
    // only two field exist , utf-8 is not checked
    //  string key = 1;
    //  string value = 2;
    for ( uint32_t i = 1 ; i <=  2; i++)
    {
        uint32_t head = 0;
        ptr = GetVarint32Ptr(ptr, ptr_end, &head);
        IF_NULL_RETURN_FALSE(ptr);
        uint32_t mode = head & 0x7;
        uint32_t index = head >> 3;
        IF_CONDITION_RETURN_FALSE(index != i || mode != 2);
        uint32_t len = 0;
        ptr = GetVarint32Ptr(ptr, ptr_end, &len);
        IF_CONDITION_RETURN_FALSE(ptr == NULL || ptr + len > ptr_end);
        if (i == 1)
        {
            key.mPtr = ptr;
            key.mLen = len;
        }
        else
        {
            value.mPtr = ptr;
            value.mLen = len;
        }
        ptr += len;
    }
    return ptr == ptr_end;
}

struct SlsTagFlatPb
{
    const char* mPtr;
    int32_t mLen;
    SlsTagFlatPb(const char* ptr, int32_t len) : mPtr(ptr), mLen(len)
    {
    }
    bool GetTagKeyValue(SlsStringPiece& key, SlsStringPiece& value) const
    {
        return ParseKeyValuePb(mPtr, mPtr + mLen , key, value);
    }
};

struct SlsLogFlatPb 
{
    const char* mPtr;
    int32_t mLen;
    uint32_t mTime;
    uint32_t mTimeNanoPart{(uint32_t)-1};

    SlsLogFlatPb(const char* ptr, int32_t len) : mPtr(ptr), mLen(len), mTime(0)
    {
    }
    uint32_t GetTime() const
    {
        return mTime;
    }

    bool HasTimeNs() const {
      return mTimeNanoPart < std::nano::den;
    }

    uint32_t GetTimeNanoPart() const
    {
        return mTimeNanoPart;
    }

    bool Decode()
    {
        const char* ptr = mPtr;
        const char* ptr_end = mPtr + mLen;
        while (ptr < ptr_end)
        {
            uint32_t head = 0;
            ptr = GetVarint32Ptr(ptr, ptr_end, &head);
            IF_NULL_RETURN_FALSE(ptr);
            uint32_t index = head >> 3;
            if (index == 1)
            {
                ptr = GetVarint32Ptr(ptr, ptr_end, &mTime);
                IF_NULL_RETURN_FALSE(ptr);
            }
            else if (index == 2||index == 3)
            {
                uint32_t len = 0;
                ptr = GetVarint32Ptr(ptr, ptr_end, &len);
                if (ptr == NULL || ptr + len > ptr_end)
                {
                    return false;
                }
                ptr += len;
            }
            else if (index == 4) 
            {
                mTimeNanoPart = *(uint32_t*)ptr;
                ptr += 4;
            }
            else
            {
                // jump out while
                break;
            }
        }
        return mTime>0;
    }


    struct SlsLogFlatPbReader
    {
        private : 
            const char* mPtr;
            const char* mEndPtr;
        public :
            SlsLogFlatPbReader(const char* ptr, const char* ptr_end) : mPtr(ptr), mEndPtr(ptr_end)
        {
        }

            bool GetNextKeyValue(SlsStringPiece& key, SlsStringPiece& value)
            {
                while (mPtr < mEndPtr)
                {
                    uint32_t head = 0;
                    mPtr = GetVarint32Ptr(mPtr, mEndPtr, &head);
                    IF_NULL_RETURN_FALSE(mPtr);
                    uint32_t mode = head & 0x7;
                    uint32_t index = head >> 3;
                    if (index == 1) 
                    {
                        uint32_t data_time = 0;
                        mPtr = GetVarint32Ptr(mPtr, mEndPtr, &data_time);
                        IF_NULL_RETURN_FALSE(mPtr);
                    } 
                    else if (index == 2||index==3) 
                    {
                        key.mLen = 0;
                        value.mLen = 0;
                        IF_CONDITION_RETURN_FALSE(mode != 2);
                        uint32_t len = 0;
                        mPtr = GetVarint32Ptr(mPtr, mEndPtr, &len);
                        IF_CONDITION_RETURN_FALSE(mPtr == NULL || mPtr + len > mEndPtr);
                        IF_FAIL_RETURN_FALSE(ParseKeyValuePb(mPtr, mPtr + len, key, value));
                        mPtr += len;
                        return true;
                    }
                    else if (index == 4)
                    {
                        mPtr += 4;
                    }
                    else 
                    {
                      // jump out while
                        break;
                    }
                }
                return false;
            }
    };

    SlsLogFlatPbReader GetReader() const
    {
        return SlsLogFlatPbReader(mPtr, mPtr + mLen);
    }
};


struct SlsLogGroupFlatPb
{
    const char* mDataPtr;
    int32_t mDataLen;
    std::vector<SlsLogFlatPb> mFlatLogs;
    std::vector<SlsPbOffsetLen> mTagsOffset;
    bool mInitFromLogGroupList;
    SlsPbOffsetLen mTopic;
    SlsPbOffsetLen mSource;
    SlsLogGroupFlatPb(const char* data_ptr, int32_t data_len, bool initFromLogGroupList = false) : 
        mDataPtr(data_ptr), mDataLen(data_len), mInitFromLogGroupList(initFromLogGroupList)
    {
    }

    ~SlsLogGroupFlatPb()
    {
        if (!mInitFromLogGroupList)
        {
            delete mDataPtr;
            mDataPtr = NULL;
        }
    }
    
    bool Decode()
    {
        const char* ptr = mDataPtr;
        const char* ptr_end = ptr + mDataLen;
        while (ptr < ptr_end)
        {
            uint32_t head = 0;
            ptr = GetVarint32Ptr(ptr, ptr_end, &head);
            IF_NULL_RETURN_FALSE(ptr);
            uint32_t mode = head & 0x7;
            uint32_t index = head >> 3;
            if ( index >= 1 && index <= 6)
            {
                // all the mode for index [1, 6] should be 2
                IF_CONDITION_RETURN_FALSE(mode !=2);
                uint32_t len = 0;
                ptr = GetVarint32Ptr(ptr, ptr_end,  &len);
                IF_CONDITION_RETURN_FALSE (ptr == NULL || ptr + len > ptr_end)
                if (index == 1)
                {
                    SlsLogFlatPb flat_pb(ptr, len);
                    if (flat_pb.Decode() == false)
                    {
                        return false;
                    }
                    mFlatLogs.push_back(flat_pb);
                }
                else if (index == 3)
                {
                    mTopic.mOffset = ptr - mDataPtr;
                    mTopic.mLen = len;
                }
                else if (index == 4)
                {
                    mSource.mOffset = ptr - mDataPtr;
                    mSource.mLen = len;
                }
                else if ( index == 6)
                {
                    SlsPbOffsetLen offset_len(ptr - mDataPtr, len);
                    mTagsOffset.push_back(offset_len);
                }
                ptr += len;
            }
            else
            {
                break;  // it should never happen
            }
        }
        return true;
    }
    int32_t GetLogsSize() const
    {
        return mFlatLogs.size();
    
    }
    const SlsLogFlatPb& GetLogFlatPb(int32_t i) const
    {
        if (i >= 0 && i < (int32_t)mFlatLogs.size())
        {
            return mFlatLogs[i];
        }
        const static SlsLogFlatPb s_empty_pb(NULL,0);
        return s_empty_pb;
    }
    int32_t GetTagsSize() const
    {
        return mTagsOffset.size();
    }
    SlsTagFlatPb GetTagFlatPb(int32_t i) const
    {
        if (i >= 0 && i < (int32_t)mTagsOffset.size())
        {
            const SlsPbOffsetLen& offset_len = mTagsOffset[i];
            return SlsTagFlatPb(mDataPtr + offset_len.mOffset, offset_len.mLen);
        }
        return SlsTagFlatPb(NULL, 0);
    }
    bool GetTopic(SlsStringPiece& topic) const
    {
        if (mTopic.mLen > 0)
        {
            topic.mPtr = mDataPtr + mTopic.mOffset;
            topic.mLen = mTopic.mLen;
            return true;
        }
        return false;
    }
    bool GetTopic(const char*& ptr, int32_t& len)
    {
        if (mTopic.mLen > 0)
        {
            ptr = mDataPtr + mTopic.mOffset;
            len = mTopic.mLen;
            return true;
        }
        return false;
    }
    std::string GetTopic(void) const
    {
        if (mTopic.mLen > 0)
        {
            return std::string(mDataPtr + mTopic.mOffset, mTopic.mLen);
        }
        return "";
    }
    bool GetSource(SlsStringPiece& source) const 
    {
        if (mSource.mLen > 0)
        {
            source.mPtr = mDataPtr + mSource.mOffset;
            source.mLen = mSource.mLen;
            return true;
        }
        return false;
    }
    bool GetSource(const char*& ptr, int32_t& len)
    {
        if (mSource.mLen > 0)
        {
            ptr = mDataPtr + mSource.mOffset;
            len = mSource.mLen;
            return true;
        }
        return false;
    }
    std::string GetSource(void) const
    {
        if (mSource.mLen > 0)
        {
            return std::string(mDataPtr + mSource.mOffset, mSource.mLen);
        }
        return "";
    }
};

struct SlsLogGroupListFlatPb
{
    const char* mDataPtr;
    int32_t mDataLen;
    std::vector<SlsLogGroupFlatPb> mFlatLogGroups;
    SlsLogGroupListFlatPb(const char* data_ptr, int32_t data_len) : 
        mDataPtr(data_ptr), mDataLen(data_len)
    {
    }
    
    bool Decode()
    {
        const char* ptr = mDataPtr;
        const char* ptr_end = ptr + mDataLen;
        while (ptr < ptr_end)
        {
            uint32_t head = 0;
            ptr = GetVarint32Ptr(ptr, ptr_end, &head);
            IF_NULL_RETURN_FALSE(ptr);
            uint32_t mode = head & 0x7;
            uint32_t index = head >> 3;
            if (index == 1)
            {
                IF_CONDITION_RETURN_FALSE(mode !=2);
                uint32_t len = 0;
                ptr = GetVarint32Ptr(ptr, ptr_end,  &len);
                IF_CONDITION_RETURN_FALSE (ptr == NULL || ptr + len > ptr_end);
                SlsLogGroupFlatPb flat_pb(ptr, len, true);
                if (flat_pb.Decode() == false)
                {
                    return false;
                }
                mFlatLogGroups.push_back(flat_pb);
                ptr += len;
            }
            else
            {
                break;  // it should never happen
            }
        }
        return true;
    }
    
    uint32_t GetLogGroupSize() const
    {
        return mFlatLogGroups.size();
    
    }
    const SlsLogGroupFlatPb& GetLogGroupFlatPb(int32_t i) const
    {
        if (i >= 0 && i < (int32_t)mFlatLogGroups.size())
        {
            return mFlatLogGroups[i];
        }
        const static SlsLogGroupFlatPb s_empty_pb(NULL,0);
        return s_empty_pb;
    }
};


#undef IF_NULL_RETURN_FALSE
#undef IF_FAIL_RETURN_FALSE
#undef IF_CONDITION_RETURN_FALSE
}
}


#endif

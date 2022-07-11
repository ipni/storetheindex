// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: finder.proto

package reqresp_pb

import (
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type FinderMessage_MessageType int32

const (
	FinderMessage_ERROR_RESPONSE          FinderMessage_MessageType = 0
	FinderMessage_FIND                    FinderMessage_MessageType = 1
	FinderMessage_FIND_RESPONSE           FinderMessage_MessageType = 2
	FinderMessage_LIST_PROVIDERS          FinderMessage_MessageType = 3
	FinderMessage_LIST_PROVIDERS_RESPONSE FinderMessage_MessageType = 4
	FinderMessage_GET_PROVIDER            FinderMessage_MessageType = 5
	FinderMessage_GET_PROVIDER_RESPONSE   FinderMessage_MessageType = 6
	FinderMessage_GET_STATS               FinderMessage_MessageType = 7
	FinderMessage_GET_STATS_RESPONSE      FinderMessage_MessageType = 8
)

var FinderMessage_MessageType_name = map[int32]string{
	0: "ERROR_RESPONSE",
	1: "FIND",
	2: "FIND_RESPONSE",
	3: "LIST_PROVIDERS",
	4: "LIST_PROVIDERS_RESPONSE",
	5: "GET_PROVIDER",
	6: "GET_PROVIDER_RESPONSE",
	7: "GET_STATS",
	8: "GET_STATS_RESPONSE",
}

var FinderMessage_MessageType_value = map[string]int32{
	"ERROR_RESPONSE":          0,
	"FIND":                    1,
	"FIND_RESPONSE":           2,
	"LIST_PROVIDERS":          3,
	"LIST_PROVIDERS_RESPONSE": 4,
	"GET_PROVIDER":            5,
	"GET_PROVIDER_RESPONSE":   6,
	"GET_STATS":               7,
	"GET_STATS_RESPONSE":      8,
}

func (x FinderMessage_MessageType) String() string {
	return proto.EnumName(FinderMessage_MessageType_name, int32(x))
}

func (FinderMessage_MessageType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_02dfec63316bfb34, []int{0, 0}
}

type FinderMessage struct {
	// defines what type of message it is.
	Type FinderMessage_MessageType `protobuf:"varint,1,opt,name=type,proto3,enum=reqresp.pb.FinderMessage_MessageType" json:"type,omitempty"`
	// Value for the message
	Data []byte `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
}

func (m *FinderMessage) Reset()         { *m = FinderMessage{} }
func (m *FinderMessage) String() string { return proto.CompactTextString(m) }
func (*FinderMessage) ProtoMessage()    {}
func (*FinderMessage) Descriptor() ([]byte, []int) {
	return fileDescriptor_02dfec63316bfb34, []int{0}
}
func (m *FinderMessage) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *FinderMessage) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_FinderMessage.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *FinderMessage) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FinderMessage.Merge(m, src)
}
func (m *FinderMessage) XXX_Size() int {
	return m.Size()
}
func (m *FinderMessage) XXX_DiscardUnknown() {
	xxx_messageInfo_FinderMessage.DiscardUnknown(m)
}

var xxx_messageInfo_FinderMessage proto.InternalMessageInfo

func (m *FinderMessage) GetType() FinderMessage_MessageType {
	if m != nil {
		return m.Type
	}
	return FinderMessage_ERROR_RESPONSE
}

func (m *FinderMessage) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func init() {
	proto.RegisterEnum("reqresp.pb.FinderMessage_MessageType", FinderMessage_MessageType_name, FinderMessage_MessageType_value)
	proto.RegisterType((*FinderMessage)(nil), "reqresp.pb.FinderMessage")
}

func init() { proto.RegisterFile("finder.proto", fileDescriptor_02dfec63316bfb34) }

var fileDescriptor_02dfec63316bfb34 = []byte{
	// 260 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x49, 0xcb, 0xcc, 0x4b,
	0x49, 0x2d, 0xd2, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0x2a, 0x4a, 0x2d, 0x2c, 0x4a, 0x2d,
	0x2e, 0xd0, 0x2b, 0x48, 0x52, 0x5a, 0xc2, 0xc4, 0xc5, 0xeb, 0x06, 0x96, 0xf4, 0x4d, 0x2d, 0x2e,
	0x4e, 0x4c, 0x4f, 0x15, 0xb2, 0xe4, 0x62, 0x29, 0xa9, 0x2c, 0x48, 0x95, 0x60, 0x54, 0x60, 0xd4,
	0xe0, 0x33, 0x52, 0xd5, 0x43, 0x28, 0xd6, 0x43, 0x51, 0xa8, 0x07, 0xa5, 0x43, 0x2a, 0x0b, 0x52,
	0x83, 0xc0, 0x5a, 0x84, 0x84, 0xb8, 0x58, 0x52, 0x12, 0x4b, 0x12, 0x25, 0x98, 0x14, 0x18, 0x35,
	0x78, 0x82, 0xc0, 0x6c, 0xa5, 0xc3, 0x8c, 0x5c, 0xdc, 0x48, 0x2a, 0x85, 0x84, 0xb8, 0xf8, 0x5c,
	0x83, 0x82, 0xfc, 0x83, 0xe2, 0x83, 0x5c, 0x83, 0x03, 0xfc, 0xfd, 0x82, 0x5d, 0x05, 0x18, 0x84,
	0x38, 0xb8, 0x58, 0xdc, 0x3c, 0xfd, 0x5c, 0x04, 0x18, 0x85, 0x04, 0xb9, 0x78, 0x41, 0x2c, 0x84,
	0x24, 0x13, 0x48, 0x83, 0x8f, 0x67, 0x70, 0x48, 0x7c, 0x40, 0x90, 0x7f, 0x98, 0xa7, 0x8b, 0x6b,
	0x50, 0xb0, 0x00, 0xb3, 0x90, 0x34, 0x97, 0x38, 0xaa, 0x18, 0x42, 0x03, 0x8b, 0x90, 0x00, 0x17,
	0x8f, 0xbb, 0x2b, 0x42, 0x4e, 0x80, 0x55, 0x48, 0x92, 0x4b, 0x14, 0x59, 0x04, 0xa1, 0x98, 0x4d,
	0x88, 0x97, 0x8b, 0x13, 0x24, 0x15, 0x1c, 0xe2, 0x18, 0x12, 0x2c, 0xc0, 0x2e, 0x24, 0xc6, 0x25,
	0x04, 0xe7, 0x22, 0x94, 0x71, 0x38, 0x49, 0x9c, 0x78, 0x24, 0xc7, 0x78, 0xe1, 0x91, 0x1c, 0xe3,
	0x83, 0x47, 0x72, 0x8c, 0x13, 0x1e, 0xcb, 0x31, 0x5c, 0x78, 0x2c, 0xc7, 0x70, 0xe3, 0xb1, 0x1c,
	0x43, 0x12, 0x1b, 0x38, 0x4c, 0x8d, 0x01, 0x01, 0x00, 0x00, 0xff, 0xff, 0xdd, 0x03, 0x37, 0x1d,
	0x63, 0x01, 0x00, 0x00,
}

func (m *FinderMessage) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *FinderMessage) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *FinderMessage) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Data) > 0 {
		i -= len(m.Data)
		copy(dAtA[i:], m.Data)
		i = encodeVarintFinder(dAtA, i, uint64(len(m.Data)))
		i--
		dAtA[i] = 0x12
	}
	if m.Type != 0 {
		i = encodeVarintFinder(dAtA, i, uint64(m.Type))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func encodeVarintFinder(dAtA []byte, offset int, v uint64) int {
	offset -= sovFinder(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *FinderMessage) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Type != 0 {
		n += 1 + sovFinder(uint64(m.Type))
	}
	l = len(m.Data)
	if l > 0 {
		n += 1 + l + sovFinder(uint64(l))
	}
	return n
}

func sovFinder(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozFinder(x uint64) (n int) {
	return sovFinder(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *FinderMessage) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowFinder
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: FinderMessage: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: FinderMessage: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Type", wireType)
			}
			m.Type = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowFinder
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Type |= FinderMessage_MessageType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowFinder
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthFinder
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthFinder
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Data = append(m.Data[:0], dAtA[iNdEx:postIndex]...)
			if m.Data == nil {
				m.Data = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipFinder(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthFinder
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipFinder(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowFinder
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowFinder
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowFinder
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthFinder
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupFinder
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthFinder
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthFinder        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowFinder          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupFinder = fmt.Errorf("proto: unexpected end of group")
)

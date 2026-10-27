#!/usr/bin/env python3
"""#242/ERR-A 错误作用域分类表与不可变参考状态模型。"""
from dataclasses import dataclass, replace
from enum import Enum
from types import MappingProxyType
from typing import Mapping


class FaultKind(str, Enum):
    """参考模型识别的错误来源。"""

    BUSINESS_REJECTION = "BUSINESS_REJECTION"
    STREAM_SEMANTIC = "STREAM_SEMANTIC"
    LOCAL_RESOURCE_PRESSURE = "LOCAL_RESOURCE_PRESSURE"
    UNTRUSTED_FRAME_BOUNDARY = "UNTRUSTED_FRAME_BOUNDARY"
    CONNECTION_NEGOTIATION = "CONNECTION_NEGOTIATION"
    TLS_INTEGRITY = "TLS_INTEGRITY"
    INTERNAL_CORRUPTION = "INTERNAL_CORRUPTION"


class Action(str, Enum):
    """分类器选择的协议处置。"""

    FINAL_STATUS = "FINAL_STATUS"
    RESET_STREAM = "RESET_STREAM"
    RESOURCE_REFUSAL = "RESOURCE_REFUSAL"
    CLOSE_CONNECTION = "CLOSE_CONNECTION"


class CallTerminal(str, Enum):
    """状态模型中单个调用记录的唯一终态。"""

    FINAL_STATUS = "FINAL_STATUS"
    RESET = "RESET"
    RESOURCE_REFUSED = "RESOURCE_REFUSED"
    CONNECTION_FAILED = "CONNECTION_FAILED"


@dataclass(frozen=True)
class ErrorRule:
    """错误分类表中的作用域、处置与剩余义务。"""

    scope: str
    action: Action
    condition: str
    obligation: str


ERROR_CLASSIFICATION_TABLE: Mapping[FaultKind, ErrorRule] = MappingProxyType({
    FaultKind.BUSINESS_REJECTION: ErrorRule(
        "调用", Action.FINAL_STATUS, "方法、业务或调用级鉴权已完成解析",
        "冻结该调用终局并停止继续接收其消息"),
    FaultKind.STREAM_SEMANTIC: ErrorRule(
        "单流或连接", Action.RESET_STREAM,
        "只有帧边界、流标识可信且 RESET 已协商时才隔离单流",
        "释放目标流存储并恰好归还一次已消费连接信用"),
    FaultKind.LOCAL_RESOURCE_PRESSURE: ErrorRule(
        "调用", Action.RESOURCE_REFUSAL, "本地预算或任务额度不足",
        "明确拒绝资源申请，不归因为对端协议损坏"),
    FaultKind.UNTRUSTED_FRAME_BOUNDARY: ErrorRule(
        "连接", Action.CLOSE_CONNECTION, "无法安全定位下一帧或流",
        "关闭连接并由所有者屏障收敛其活动调用"),
    FaultKind.CONNECTION_NEGOTIATION: ErrorRule(
        "连接", Action.CLOSE_CONNECTION, "连接能力或参数协商无效",
        "关闭连接并由所有者屏障收敛其活动调用"),
    FaultKind.TLS_INTEGRITY: ErrorRule(
        "连接", Action.CLOSE_CONNECTION, "TLS 完整性校验失败",
        "关闭连接，不回退为明文或单流恢复"),
    FaultKind.INTERNAL_CORRUPTION: ErrorRule(
        "连接", Action.CLOSE_CONNECTION, "未证明存在安全恢复域",
        "默认关闭连接；不得遍历可能已损坏的私有状态"),
})


@dataclass(frozen=True)
class ParseTrust:
    """分类时已能证明的解析与协议能力事实。"""

    frame_boundary_trusted: bool = False
    stream_identity_trusted: bool = False
    reset_supported: bool = False


def classify_error(kind: FaultKind, trust: ParseTrust = ParseTrust()) -> Action:
    """按错误来源和解析可信度选择最窄的安全处置作用域。"""
    if kind not in ERROR_CLASSIFICATION_TABLE:
        raise ValueError("未知错误类型")

    rule = ERROR_CLASSIFICATION_TABLE[kind]
    if kind is FaultKind.STREAM_SEMANTIC:
        if (trust.frame_boundary_trusted and trust.stream_identity_trusted
                and trust.reset_supported):
            return Action.RESET_STREAM
        return Action.CLOSE_CONNECTION
    return rule.action


@dataclass(frozen=True)
class CallState:
    """单个活动或已终结调用的不可变快照。"""

    call_id: str
    terminal: CallTerminal | None = None
    terminal_count: int = 0

    def __post_init__(self):
        if not self.call_id:
            raise ValueError("调用标识不能为空")
        expected_count = 0 if self.terminal is None else 1
        if self.terminal_count != expected_count:
            raise ValueError("每个调用必须保持零次或一次终局")


@dataclass(frozen=True)
class ConnectionState:
    """最多一次终局的连接与调用参考状态。"""

    calls: tuple[CallState, ...]
    is_closed: bool = False
    close_reason: FaultKind | None = None

    def __post_init__(self):
        call_ids = tuple(item.call_id for item in self.calls)
        if len(set(call_ids)) != len(call_ids):
            raise ValueError("连接内调用标识必须唯一")
        if self.is_closed != (self.close_reason is not None):
            raise ValueError("关闭状态必须同时保存关闭原因")

    @classmethod
    def open(cls, call_ids: tuple[str, ...]) -> "ConnectionState":
        """创建包含指定活动调用的开放连接快照。"""
        return cls(tuple(CallState(call_id) for call_id in call_ids))

    def apply(self, call_id: str | None, kind: FaultKind,
              trust: ParseTrust = ParseTrust()) -> tuple["ConnectionState", Action]:
        """纯状态转换：返回新快照，不修改输入连接或调用。"""
        action = classify_error(kind, trust)
        if self.is_closed:
            return self, action

        if action is Action.CLOSE_CONNECTION:
            calls = tuple(
                item if item.terminal is not None else replace(
                    item, terminal=CallTerminal.CONNECTION_FAILED,
                    terminal_count=1)
                for item in self.calls)
            return ConnectionState(calls, True, kind), action

        if call_id is None:
            raise ValueError("调用级处置必须指定活动调用")
        index = next((index for index, item in enumerate(self.calls)
                      if item.call_id == call_id), None)
        if index is None:
            raise ValueError("目标调用不存在")
        if self.calls[index].terminal is not None:
            raise ValueError("目标调用已经终结")

        terminal = {
            Action.FINAL_STATUS: CallTerminal.FINAL_STATUS,
            Action.RESET_STREAM: CallTerminal.RESET,
            Action.RESOURCE_REFUSAL: CallTerminal.RESOURCE_REFUSED,
        }[action]
        calls = list(self.calls)
        calls[index] = replace(calls[index], terminal=terminal,
                               terminal_count=1)
        return replace(self, calls=tuple(calls)), action

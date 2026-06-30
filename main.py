import asyncio
import datetime
import ipaddress
import json
import os
import re
import shutil
import time
import urllib.parse
import uuid
from pathlib import Path

import aiohttp
import filetype

import astrbot.api.message_components as Comp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.components import BaseMessageComponent, Image, Reply
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)


class MemeGrabberPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        # 获取插件数据目录（使用框架规范方法）
        default_data_dir = StarTools.get_data_dir(self.name)
        temp_dir = self.config.get("temp_dir", default_data_dir)
        # 转换为绝对路径，确保是字符串类型
        self.data_dir = os.path.abspath(str(temp_dir))

        # 校验 temp_dir 必须在插件数据目录白名单内
        default_data_dir_abs = os.path.abspath(str(default_data_dir))
        if not Path(self.data_dir).is_relative_to(Path(default_data_dir_abs)):
            logger.warning(
                f"配置的 temp_dir {self.data_dir} 不在默认数据目录 {default_data_dir_abs} 内，使用默认数据目录"
            )
            self.data_dir = default_data_dir_abs

        os.makedirs(self.data_dir, exist_ok=True)
        # 获取是否在发送后删除临时文件的配置
        self.delete_after_send = self.config.get("delete_after_send", True)
        # 获取默认图片扩展名
        self.default_extension = self.config.get("default_extension", "jpg")
        # 获取图片下载超时时间
        self.download_timeout = self.config.get("download_timeout", 60)
        # 获取发送方式: file(群文件方式) / image(图片方式)
        self.send_method = self.config.get("send_method", "image")
        # 获取自定义文件命名规则
        self.filename_pattern = self.config.get("filename_pattern", "meme_{date}_{timestamp}")
        # 群名单配置
        self.list_mode = self.config.get("list_mode", "blacklist")
        self._group_ids = self._parse_group_list(self.config.get("group_list", ""))
        # 延迟初始化 aiohttp ClientSession，首次使用时创建
        self.session = None
        # 用于保护 session 初始化的锁
        self.session_lock = asyncio.Lock()
        # 并发下载限制信号量
        self.download_semaphore = asyncio.Semaphore(5)

    async def _get_session(self):
        """
        获取或创建 aiohttp ClientSession

        Returns:
            aiohttp.ClientSession: 异步HTTP会话
        """
        async with self.session_lock:
            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession()
        return self.session

    async def download_image(self, picture_url: str, relative_path: str) -> bool:
        """
        下载图片到本地

        Args:
            picture_url: 图片URL
            relative_path: 保存路径

        Returns:
            bool: 下载是否成功
        """
        try:
            # 安全校验：确保只允许 http/https 协议
            parsed_url = urllib.parse.urlparse(picture_url)
            if parsed_url.scheme not in ("http", "https"):
                logger.error(f"不支持的URL协议: {parsed_url.scheme}")
                return False

            # 防止 SSRF 攻击：解析域名并检查是否为内网地址
            try:
                hostname = parsed_url.netloc.split(":")[0]  # 移除端口号
                # 使用异步DNS解析
                loop = asyncio.get_running_loop()
                addr_info = await loop.getaddrinfo(hostname, None)
                ip_address = addr_info[0][4][0]
                # 检查是否为内网地址
                try:
                    ip_obj = ipaddress.ip_address(ip_address)
                    if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
                        logger.error(f"禁止访问内网地址: {ip_address}")
                        return False
                except ValueError:
                    # 非有效IP地址，可能是域名
                    pass
            except Exception as e:
                logger.error(f"解析URL地址时发生错误: {str(e)}")
                return False

            # 获取或创建 ClientSession
            session = await self._get_session()
            # 使用复用的 ClientSession 进行异步请求，设置超时时间
            async with self.download_semaphore:
                async with session.get(
                    picture_url, timeout=self.download_timeout
                ) as response:
                    if response.status == 200:
                        # 流式下载，设置最大文件大小为 10MB
                        max_size = 10 * 1024 * 1024
                        current_size = 0
                        # 确保目录存在
                        os.makedirs(os.path.dirname(relative_path), exist_ok=True)
                        with open(relative_path, "wb") as f:
                            async for chunk in response.content:
                                current_size += len(chunk)
                                if current_size > max_size:
                                    logger.error("图片文件过大，超过10MB")
                                    # 清理已下载的部分文件
                                    if os.path.exists(relative_path):
                                        os.remove(relative_path)
                                        logger.info(
                                            f"已清理过大的临时文件: {relative_path}"
                                        )
                                    return False
                                f.write(chunk)
                        return True
                    else:
                        logger.error(f"下载图片失败，状态码: {response.status}")
                        return False
        except Exception as e:
            logger.exception(f"下载图片时发生错误: {str(e)}")
            # 清理可能的临时文件
            if os.path.exists(relative_path):
                os.remove(relative_path)
                logger.info(f"已删除: {os.path.basename(relative_path)}")
            return False

    async def send_file_to_user(
        self,
        event: AstrMessageEvent,
        file_path: str,
        filename: str,
        is_plugin_created: bool = True,
    ):
        """
        发送文件给用户

        Args:
            event: 消息事件
            file_path: 文件路径
            filename: 文件名
            is_plugin_created: 是否为插件创建的文件（用于控制删除行为）
        """
        try:
            # 使用 AstrBot 官方接口发送文件
            chain: list[BaseMessageComponent] = [
                self._build_send_component(file_path, filename)
            ]
            # 使用 yield 发送，保持生成器函数特性
            yield event.chain_result(chain)
        except Exception as e:
            logger.error(f"发送文件时发生错误: {str(e)}")
            yield event.plain_result(f"发送文件失败: {str(e)}")
        finally:
            # 根据配置决定是否删除临时文件，仅删除插件自己创建的文件
            if self.delete_after_send and is_plugin_created:
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        logger.info(f"已删除: {os.path.basename(file_path)}")
                except Exception as e:
                    logger.error(f"删除临时文件时发生错误: {str(e)}")
            event.stop_event()

    def _generate_filename(self, ext: str) -> str:
        """
        生成唯一的文件名

        Args:
            ext: 文件扩展名，包含点号

        Returns:
            str: 生成的文件名
        """
        now = datetime.datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H-%M-%S")
        timestamp = int(time.time() * 1000)
        unique_id = uuid.uuid4().hex[:8]

        result = self.filename_pattern
        result = re.sub(r"\{datetime:([^}]+)\}", lambda m: now.strftime(m.group(1)), result)
        result = result.replace("{date}", date_str)
        result = result.replace("{time}", time_str)
        result = result.replace("{timestamp}", str(timestamp))
        result = result.replace("{uuid}", unique_id)
        return result + ext

    # 动图格式扩展名，图片模式下自动降级为文件方式发送以保留动画
    _ANIMATED_EXTENSIONS = {".gif", ".apng", ".webp"}

    def _build_send_component(self, file_path: str, filename: str) -> BaseMessageComponent:
        """
        根据配置的发送方式构建消息组件

        Args:
            file_path: 文件路径
            filename: 文件名

        Returns:
            BaseMessageComponent: 文件组件或图片组件
        """
        if self.send_method == "image":
            ext = os.path.splitext(filename)[1].lower()
            if ext in self._ANIMATED_EXTENSIONS:
                return Comp.File(file=file_path, name=filename)
            return Comp.Image.fromFileSystem(file_path)
        return Comp.File(file=file_path, name=filename)

    def _mode_name(self) -> str:
        """返回当前名单模式的中文名称"""
        return "黑名单" if self.list_mode == "blacklist" else "白名单"

    def _cleanup_temp_files(self, temp_files: list):
        """根据配置清理临时文件"""
        if not self.delete_after_send:
            return
        for file_path in temp_files:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"已删除: {os.path.basename(file_path)}")
            except Exception as e:
                logger.exception(f"删除临时文件时发生错误: {str(e)}")

    async def _build_image_tasks(self, image_components: list) -> list:
        """从 Image 组件列表构建 _process_image 的异步任务列表"""
        tasks = []
        for img in image_components:
            url = img.url or img.file or ""
            if url and (url.startswith("http://") or url.startswith("https://")):
                tasks.append(self._process_image(picture_url=url))
            else:
                local_path = await img.convert_to_file_path()
                tasks.append(self._process_image(local_path=local_path))
        return tasks

    def _build_chain_from_results(self, results: list) -> tuple[list, list]:
        """从 _process_image 结果列表构建消息链和临时文件列表"""
        chain: list[BaseMessageComponent] = []
        temp_files = []
        for file_path, filename, is_temp in results:
            if file_path and filename:
                chain.append(self._build_send_component(file_path, filename))
                if is_temp:
                    temp_files.append(file_path)
        return chain, temp_files

    @staticmethod
    def _parse_group_list(group_list_str: str) -> set:
        """解析群号列表字符串为集合"""
        if not group_list_str or not group_list_str.strip():
            return set()
        return {line.strip() for line in group_list_str.strip().splitlines() if line.strip()}

    def _check_group_allowed(self, group_id: str) -> bool:
        """检查群聊是否允许使用功能"""
        if self.list_mode == "disabled":
            return True
        if not group_id:
            return True  # 私聊不受限制
        in_list = group_id in self._group_ids
        if self.list_mode == "blacklist":
            return not in_list
        if self.list_mode == "whitelist":
            return in_list
        return True

    def _save_group_list(self):
        """保存群名单到配置文件"""
        self.config["group_list"] = "\n".join(sorted(self._group_ids))
        # 尝试触发配置持久化
        try:
            self.config.save()
        except Exception:
            logger.warning("自动保存配置失败，请手动在管理面板保存")

    async def _process_image(self, picture_url=None, local_path=None):
        """
        统一处理图片，转换为可发送的文件。picture_url 优先于 local_path。

        Args:
            picture_url: 图片URL（优先使用）
            local_path: 本地文件路径

        Returns:
            tuple: (文件路径, 文件名, 是否为临时文件)
        """
        try:
            if picture_url:
                # 下载URL图片
                parsed_url = urllib.parse.urlparse(picture_url)
                path = parsed_url.path
                file_ext = os.path.splitext(path)[1].lower()
                if not file_ext:
                    file_ext = f".{self.default_extension}"

                filename = self._generate_filename(file_ext)
                relative_path = os.path.join(self.data_dir, filename)

                success = await self.download_image(picture_url, relative_path)
                if success:
                    return os.path.abspath(relative_path), filename, True
                logger.error(f"下载图片失败: {picture_url}")
                return None, None, False

            if local_path:
                # 处理本地文件
                file_extension = f".{self.default_extension}"
                try:
                    kind = filetype.guess(local_path)
                    if kind and kind.extension:
                        file_extension = f".{kind.extension}"
                except Exception:
                    logger.exception("使用filetype判断图片类型失败")

                filename = self._generate_filename(file_extension)
                temp_path = os.path.join(self.data_dir, filename)

                try:
                    shutil.copy2(local_path, temp_path)
                    return os.path.abspath(temp_path), filename, True
                except Exception:
                    logger.exception("复制图片到临时目录失败")
                    return os.path.abspath(local_path), filename, False

            logger.error("图片处理缺少URL和本地路径")
            return None, None, False
        except Exception:
            logger.exception("处理图片时发生错误")
            return None, None, False

    async def handle_reply_message(self, event: AstrMessageEvent, reply_msg: Reply):
        """
        处理回复消息

        Args:
            event: 消息事件
            reply_msg: 回复消息
        """
        try:
            if not isinstance(event, AiocqhttpMessageEvent):
                yield event.plain_result("抱歉，该功能仅支持 QQ 平台")
                event.stop_event()
                event.should_call_llm(False)
                return

            # 群名单检查
            group_id = event.get_group_id() if event.get_group_id() else ""
            if not self._check_group_allowed(group_id):
                yield event.plain_result("该群聊已禁止使用此功能")
                event.stop_event()
                event.should_call_llm(False)
                return

            found_images = []
            temp_files = []

            # 优先从 reply_msg.chain 获取图片（框架已解析好的消息段）
            if reply_msg.chain and isinstance(reply_msg.chain, list):
                image_components = [
                    msg for msg in reply_msg.chain
                    if isinstance(msg, Image)
                ]
                if image_components:
                    tasks = await self._build_image_tasks(image_components)
                    results = await asyncio.gather(*tasks)
                    for file_path, filename, is_temp in results:
                        if file_path and filename:
                            found_images.append((file_path, filename))
                            if is_temp:
                                temp_files.append(file_path)

            # 如果 chain 中没有找到图片，回退到 get_msg API
            if not found_images:
                client = event.bot
                response = await client.api.call_action("get_msg", message_id=reply_msg.id)
                reply_msg_content = response.get("message", [])
                if not reply_msg_content:
                    yield event.plain_result("引用消息格式错误")
                    event.stop_event()
                    event.should_call_llm(False)
                    return

                tasks = []
                for msg in reply_msg_content:
                    msg_type = msg.get("type", "")
                    if msg_type in ("image", "mface"):
                        url = msg.get("data", {}).get("url")
                        file_id = msg.get("data", {}).get("file")
                        if url:
                            tasks.append(self._process_image(picture_url=url))
                        elif file_id:
                            img_response = await client.api.call_action("get_image", file=file_id)
                            tasks.append(self._process_image(local_path=img_response.get("file")))
                        else:
                            tasks.append(self._process_image())

                results = await asyncio.gather(*tasks)
                for file_path, filename, is_temp in results:
                    if file_path and filename:
                        found_images.append((file_path, filename))
                        if is_temp:
                            temp_files.append(file_path)

            if not found_images:
                yield event.plain_result("引用消息中未找到图片")
                event.stop_event()
                event.should_call_llm(False)
                return

            chain: list[BaseMessageComponent] = []
            for file_path, filename in found_images:
                chain.append(self._build_send_component(file_path, filename))

            try:
                yield event.chain_result(chain)
            except Exception as e:
                logger.exception(f"发送文件时发生错误: {str(e)}")
                yield event.plain_result(f"发送文件失败: {str(e)}")
            finally:
                self._cleanup_temp_files(temp_files)

            event.stop_event()
        except Exception as e:
            logger.exception(f"处理回复消息时发生错误: {str(e)}")
            yield event.plain_result(f"处理回复失败: {str(e)}")
            event.stop_event()
            event.should_call_llm(False)

    @filter.command("meme", alias=["提取"])
    async def meme_command(self, event: AstrMessageEvent):
        """提取表情包为可保存的文件格式"""
        event.should_call_llm(False)
        message_chain = event.get_messages()

        # 检查平台是否支持
        if event.get_platform_name() != "aiocqhttp":
            yield event.plain_result("抱歉，该功能仅支持 QQ 平台")
            event.stop_event()
            event.should_call_llm(False)
            return

        # 群名单检查
        group_id = event.get_group_id() if event.get_group_id() else ""
        if not self._check_group_allowed(group_id):
            yield event.plain_result("该群聊已禁止使用此功能")
            event.stop_event()
            event.should_call_llm(False)
            return

        found_images = []

        # 先收集所有图片和回复
        for msg in message_chain:
            if msg.type == "Image" and isinstance(msg, Image):
                found_images.append(msg)
            elif msg.type == "Reply" and isinstance(msg, Reply):
                # 处理回复消息（已经支持多个图片）
                async for result in self.handle_reply_message(event, msg):
                    yield result
                # 回复消息处理完成后直接返回
                return

        # 处理收集到的所有图片
        if found_images:
            tasks = await self._build_image_tasks(found_images)
            results = await asyncio.gather(*tasks)
            chain, temp_files = self._build_chain_from_results(results)

            try:
                # 发送所有文件
                yield event.chain_result(chain)
            except Exception as e:
                logger.exception(f"发送文件时发生错误: {str(e)}")
                yield event.plain_result(f"发送文件失败: {str(e)}")
            finally:
                self._cleanup_temp_files(temp_files)

            event.stop_event()
            return

        # 没有找到图片
        yield event.plain_result("请引用表情包或在对话中包含表情包")
        event.stop_event()
        event.should_call_llm(False)

    def _parse_memes_arg(self, event: AstrMessageEvent, cmd: str) -> str:
        """解析 memes 子命令参数（去掉指令前缀，提取群号）。无参数时返回当前群号"""
        s = event.message_str.strip()
        for prefix in (f"/memes {cmd} ", f"{cmd} "):
            if prefix in s:
                arg = s.split(prefix, 1)[1].strip()
                return arg if arg else event.get_group_id() or ""
        return event.get_group_id() or ""

    @filter.command_group("memes")
    async def memes_command_group(self, event: AstrMessageEvent):
        ...

    @memes_command_group.command("add")
    async def memes_add(self, event: AstrMessageEvent):
        """将群聊加入名单，可指定群号 /memes add 123456"""
        event.should_call_llm(False)
        target_id = self._parse_memes_arg(event, "add")
        if not target_id:
            yield event.plain_result("请在群聊中使用此命令，或指定群号，如 /memes add 123456")
            return
        if target_id in self._group_ids:
            mn = self._mode_name()
            yield event.plain_result(f"该群聊已在{mn}中")
            return
        self._group_ids.add(target_id)
        self._save_group_list()
        yield event.plain_result(f"已添加群 {target_id} 到{self._mode_name()}")

    @memes_command_group.command("del")
    async def memes_del(self, event: AstrMessageEvent):
        """从名单中移除群聊，可指定群号 /memes del 123456"""
        event.should_call_llm(False)
        target_id = self._parse_memes_arg(event, "del")
        if not target_id:
            yield event.plain_result("请在群聊中使用此命令，或指定群号，如 /memes del 123456")
            return
        if target_id not in self._group_ids:
            yield event.plain_result("该群聊不在名单中")
            return
        self._group_ids.discard(target_id)
        self._save_group_list()
        yield event.plain_result(f"已从{self._mode_name()}中移除群 {target_id}")

    @memes_command_group.command("list")
    async def memes_list(self, event: AstrMessageEvent):
        """查看当前名单状态和群号列表"""
        event.should_call_llm(False)
        mode_names = {"blacklist": "黑名单模式", "whitelist": "白名单模式", "disabled": "已禁用"}
        mode_label = mode_names.get(self.list_mode, self.list_mode)
        ids = sorted(self._group_ids)
        if ids:
            lines = "\n".join(ids)
            yield event.plain_result(f"【{mode_label}】\n当前名单群号（{len(ids)} 个）：\n{lines}")
        else:
            yield event.plain_result(f"【{mode_label}】\n名单为空")

    @memes_command_group.command("mode")
    async def memes_mode(self, event: AstrMessageEvent):
        """切换群名单模式 /memes mode [blacklist|whitelist|disabled]"""
        event.should_call_llm(False)
        new_mode = self._parse_memes_arg(event, "mode")

        valid_modes = {"b": "黑名单", "w": "白名单", "d": "禁用"}
        if new_mode not in valid_modes:
            yield event.plain_result("请指定模式: b(黑名单) / w(白名单) / d(禁用)")
            return

        mode_map = {"b": "blacklist", "w": "whitelist", "d": "disabled"}
        self.list_mode = mode_map[new_mode]
        self.config["list_mode"] = mode_map[new_mode]
        try:
            self.config.save()
        except Exception:
            pass
        yield event.plain_result(f"群名单模式已切换为：{valid_modes[new_mode]}")

    async def terminate(self):
        """插件终止时的清理操作"""
        # 关闭 aiohttp ClientSession
        if self.session is not None and not self.session.closed:
            try:
                await self.session.close()
                logger.info("已关闭 aiohttp ClientSession")
            except Exception as e:
                logger.exception(f"关闭 ClientSession 时发生错误: {str(e)}")

        # 只有当开启了清理临时文件时才执行清理操作
        if self.delete_after_send:
            # 清理可能遗留的临时图片文件（仅删除插件生成的 meme_ 前缀文件）
            try:
                if os.path.exists(self.data_dir):
                    for file in os.listdir(self.data_dir):
                        if file.startswith("meme_"):
                            file_path = os.path.join(self.data_dir, file)
                            if os.path.isfile(file_path):
                                os.remove(file_path)
                                logger.info(f"已删除: {os.path.basename(file_path)}")
            except Exception as e:
                logger.exception(f"清理临时文件时发生错误: {str(e)}")
        else:
            logger.info("未开启清理临时文件，跳过清理操作")

    async def on_unload(self):
        """框架卸载插件时的钩子方法"""
        await self.terminate()

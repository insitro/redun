import argparse

from redun.backends.db import Tag
from redun.console.parser import format_args, parse_args
from redun.console.utils import get_links


def test_format_args() -> None:
    """
    Ensure we can parse and format argv into argparse.Namespace and back.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--flag", action="store_true")
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--file", action="append", default=[])

    argv = ["--flag", "--page", "3", "--file", "aa", "--file", "bb"]
    args = parse_args(parser, argv)
    assert args.flag
    assert args.page == 3
    assert args.file == ["aa", "bb"]
    assert format_args(parser, args) == argv

    argv = []
    args = parse_args(parser, argv)
    assert not args.flag
    assert args.page == 1
    assert args.file == []
    assert format_args(parser, args) == argv


def test_format_link() -> None:
    """
    Ensure tags can be used to generate links.
    """
    tags_dict = {
        "git_origin_url": "git@github.com:acme/workflow.git",
        "git_origin2_url": "https://github.com/acme/workflow.git",
        "git_commit": "96ab8a09853746fdc8b8b408a9de3647a92a9450",
        "aws_batch_job": "8d90f8fd-77fb-46d8-b099-bb31019f47da",
        "aws_log_stream": "workflow-redun-jd/default/ee83478a1d3643ce9ed6ad5b05b9c7a1",
    }
    tags = [Tag(key=key, value=value) for key, value in tags_dict.items()]

    link_patterns = [
        r"https://github.com/{{git_origin_url:.*github\.com:(.*)\.git$}}/commits/{{git_commit}}",
        r"https://github2.com/{{git_origin2_url:.*github\.com(:|/)(?P<val>.*)\.git$}}/commits/{{git_commit}}",  # noqa: E501
        r"https://console.aws.amazon.com/batch/home?#jobs/detail/{{aws_batch_job}}",
        r"https://console.aws.amazon.com/cloudwatch/home?#logEventViewer:group=/aws/batch/job;stream={{aws_log_stream}}",  # noqa: E501
        r"https://foo.com/{{foo}}",
        r"https://github.com/{{git_origin_url:.*bad-url.com:(.*)\.git$}}/commits/{{git_commit}}",
    ]

    assert get_links(link_patterns, tags) == [
        "https://github.com/acme/workflow/commits/96ab8a09853746fdc8b8b408a9de3647a92a9450",
        "https://github2.com/acme/workflow/commits/96ab8a09853746fdc8b8b408a9de3647a92a9450",
        "https://console.aws.amazon.com/batch/home?#jobs/detail/8d90f8fd-77fb-46d8-b099-bb31019f47da",  # noqa: E501
        "https://console.aws.amazon.com/cloudwatch/home?#logEventViewer:group=/aws/batch/job;stream=workflow-redun-jd/default/ee83478a1d3643ce9ed6ad5b05b9c7a1",  # noqa: E501
    ]

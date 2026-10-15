ARG GO_IMAGE=mcr.microsoft.com/devcontainers/go:2-1.27-trixie
FROM ${GO_IMAGE}

ARG CGO_LIBS=""

RUN apt-get update \
 && export DEBIAN_FRONTEND=noninteractive \
 && apt-get install -y --no-install-recommends \
      build-essential \
      pkg-config \
      gdb \
      tmux \
      zsh \
      git \
      jq \
      make \
      ${CGO_LIBS} \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p \
      /home/vscode/.claude \
      /home/vscode/.cache/go-build \
      /home/vscode/.cache/JetBrains \
      /home/vscode/.shell-history \
      /go/pkg/mod \
      /go/pkg/sumdb \
 && chown -R vscode:vscode /home/vscode/.claude /home/vscode/.cache /home/vscode/.shell-history /go/pkg

USER vscode
RUN OMZ=/home/vscode/.oh-my-zsh \
      && if [ ! -d "$OMZ" ]; then git clone --depth 1 https://github.com/ohmyzsh/ohmyzsh.git "$OMZ"; fi \
      && rm -rf "$OMZ/custom/plugins/zsh-autosuggestions" "$OMZ/custom/plugins/zsh-syntax-highlighting" \
      && git clone --depth 1 https://github.com/zsh-users/zsh-autosuggestions.git "$OMZ/custom/plugins/zsh-autosuggestions" \
      && git clone --depth 1 https://github.com/zsh-users/zsh-syntax-highlighting.git "$OMZ/custom/plugins/zsh-syntax-highlighting"
USER root

COPY --chown=vscode:vscode zshrc /home/vscode/.zshrc
RUN usermod --shell /usr/bin/zsh vscode

COPY --chown=vscode:vscode tmux.conf /home/vscode/.tmux.conf

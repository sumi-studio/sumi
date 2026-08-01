def BUILD_TARGETS = []

pipeline {
  agent any

  options {
    timeout(time: 30, unit: 'MINUTES')
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  }

  environment {
    REGISTRY  = 'ghcr.io'
    NAMESPACE = 'ghcr.io/sumi-studio'
    PREFIX    = 'sumi'
  }

  stages {
    stage('Prepare') {
      steps {
        script {
          env.SHORT_SHA = sh(
            script: 'git rev-parse --short HEAD',
            returnStdout: true
          ).trim()

          def found = sh(
            script: '''
              find . -name Dockerfile -type f \
                -not -path './.git/*' \
                -not -path '*/node_modules/*' \
                -not -path '*/vendor/*' \
                | sed 's|^\\./||' | sort
            ''',
            returnStdout: true
          ).trim()

          if (!found) {
            error 'Dockerfile が 1 件も見つかりません'
          }

          def discovered = found.readLines().collect { dockerfile ->
            if (!(dockerfile ==~ /[A-Za-z0-9._\/-]+/)) {
              error "安全でない文字を含む Dockerfile パスです: ${dockerfile}"
            }

            def dir = dockerfile.contains('/')
              ? dockerfile.substring(0, dockerfile.lastIndexOf('/'))
              : '.'
            def name = (dir == '.')
              ? 'root'
              : dir.substring(dir.lastIndexOf('/') + 1)

            if (!(name.toLowerCase() ==~ /[a-z0-9]+(?:[._-][a-z0-9]+)*/)) {
              error "コンテナイメージ名に使用できないディレクトリ名です: ${name}"
            }

            // Sumi keeps build recipes under deploy/<service> and source under
            // apps/<service>. Watch both while retaining automatic discovery.
            def watchedDirs = [dir]
            def appDir = "apps/${name}"
            if (dir.startsWith('deploy/') &&
                sh(script: "test -d '${appDir}'", returnStatus: true) == 0) {
              watchedDirs << appDir
            }

            [
              name: name,
              dockerfile: dockerfile,
              watchedDirs: watchedDirs
            ]
          }

          def duplicateNames = discovered
            .groupBy { it.name.toLowerCase() }
            .findAll { imageName, entries -> entries.size() > 1 }
          if (duplicateNames) {
            def details = duplicateNames.collect { imageName, entries ->
              "${imageName}: ${entries.collect { it.dockerfile }.join(', ')}"
            }.join('; ')
            error "イメージ名が衝突しています。明示リスト方式へ切り替えてください: ${details}"
          }

          // null means that a safe comparison base is unavailable, so every
          // discovered image must be rebuilt. Manual "Build Now" runs are also
          // full rebuilds, which provides the documented escape hatch for
          // shared files that are outside a service's watched directories.
          // An empty list is a valid diff for an SCM-poll-triggered build.
          List<String> changedPaths = null
          def manuallyStarted = !currentBuild
            .getBuildCauses('hudson.model.Cause$UserIdCause')
            .isEmpty()
          if (!manuallyStarted) {
            def previous = env.GIT_PREVIOUS_SUCCESSFUL_COMMIT?.trim()
            if (previous && previous ==~ /[0-9a-fA-F]{7,40}/) {
              def previousExists = sh(
                script: "git cat-file -e '${previous}^{commit}'",
                returnStatus: true
              ) == 0
              if (previousExists) {
                def diff = sh(
                  script: "git diff --name-only '${previous}' HEAD",
                  returnStdout: true
                ).trim()
                changedPaths = diff ? diff.readLines() : []
              }
            }
          }

          def targets = discovered.findAll { target ->
            changedPaths == null || changedPaths.any { path ->
              target.watchedDirs.any { watchedDir ->
                watchedDir == '.' ||
                  path == watchedDir ||
                  path.startsWith("${watchedDir}/")
              }
            }
          }

          if (targets.isEmpty()) {
            currentBuild.result = 'SUCCESS'
            currentBuild.description = '変更対象なし（スキップ）'
            env.SKIP_ALL = 'true'
          } else {
            env.SKIP_ALL = 'false'
            currentBuild.displayName =
              "#${env.BUILD_NUMBER} ${env.SHORT_SHA} [${targets.collect { it.name }.join(',')}]"
          }

          BUILD_TARGETS = targets
        }
      }
    }

    stage('Login') {
      when {
        environment name: 'SKIP_ALL', value: 'false'
      }
      steps {
        withCredentials([usernamePassword(
          credentialsId: 'registry-cred',
          usernameVariable: 'REG_USER',
          passwordVariable: 'REG_PASS'
        )]) {
          sh 'printf %s "$REG_PASS" | docker login "$REGISTRY" -u "$REG_USER" --password-stdin'
        }
      }
    }

    stage('Build & Push') {
      when {
        environment name: 'SKIP_ALL', value: 'false'
      }
      steps {
        script {
          def branches = [:]
          BUILD_TARGETS.each { item ->
            def target = item
            branches[target.name] = {
              def image = "${env.NAMESPACE}/${env.PREFIX}-${target.name}".toLowerCase()
              sh """
                set -eu
                docker build \\
                  -f '${target.dockerfile}' \\
                  -t '${image}:${env.SHORT_SHA}' \\
                  -t '${image}:latest' \\
                  .
                docker push '${image}:${env.SHORT_SHA}'
                docker push '${image}:latest'
              """
            }
          }
          parallel branches
        }
      }
    }
  }

  post {
    always {
      sh 'docker logout "$REGISTRY" || true'
      sh 'docker image prune -f || true'
    }
  }
}

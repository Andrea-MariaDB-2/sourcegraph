import classNames from 'classnames'
import ArrowDownIcon from 'mdi-react/ArrowDownIcon'
import ArrowUpIcon from 'mdi-react/ArrowUpIcon'
import ContentDuplicateIcon from 'mdi-react/ContentDuplicateIcon'
import DeleteIcon from 'mdi-react/DeleteIcon'
import PlayCircleOutlineIcon from 'mdi-react/PlayCircleOutlineIcon'
import * as Monaco from 'monaco-editor'
import React, { useState, useCallback, useRef, useMemo } from 'react'
import { useLocation } from 'react-router'
import { Observable, of } from 'rxjs'

import { LoadingSpinner } from '@sourcegraph/react-loading-spinner'
import { FetchFileParameters } from '@sourcegraph/shared/src/components/CodeExcerpt'
import { SettingsCascadeProps } from '@sourcegraph/shared/src/settings/settings'
import { TelemetryProps } from '@sourcegraph/shared/src/telemetry/telemetryService'
import { ThemeProps } from '@sourcegraph/shared/src/theme'
import { useObservable } from '@sourcegraph/shared/src/util/useObservable'
import { MonacoEditor } from '@sourcegraph/web/src/components/MonacoEditor'

import { SOURCEGRAPH_SEARCH } from '../input/MonacoQueryInput'
import { StreamingSearchResultsList } from '../results/StreamingSearchResultsList'

import blockStyles from './SearchNotebookBlock.module.scss'
import { SearchNotebookBlockMenu } from './SearchNotebookBlockMenu'
import styles from './SearchNotebookQueryBlock.module.scss'
import { useBlockSelection } from './useBlockSelection'
import { useBlockShortcuts } from './useBlockShortcuts'
import { MONACO_BLOCK_INPUT_OPTIONS, useMonacoBlockInput } from './useMonacoBlockInput'

import { BlockProps, QueryBlock } from '.'

interface SearchNotebookQueryBlockProps
    extends BlockProps,
        Omit<QueryBlock, 'type'>,
        ThemeProps,
        SettingsCascadeProps,
        TelemetryProps {
    isMacPlatform: boolean
    fetchHighlightedFileLineRanges: (parameters: FetchFileParameters, force?: boolean) => Observable<string[][]>
}

// TODO: Use React.memo
export const SearchNotebookQueryBlock: React.FunctionComponent<SearchNotebookQueryBlockProps> = ({
    id,
    input,
    output,
    isLightTheme,
    telemetryService,
    settingsCascade,
    isSelected,
    isMacPlatform,
    fetchHighlightedFileLineRanges,
    onRunBlock,
    onBlockInputChange,
    onSelectBlock,
    onMoveBlockSelection,
    onDeleteBlock,
    onMoveBlock,
    onDuplicateBlock,
}) => {
    const [editor, setEditor] = useState<Monaco.editor.IStandaloneCodeEditor>()
    const blockElement = useRef<HTMLDivElement>(null)
    const searchResults = useObservable(output ?? of(undefined))
    const location = useLocation()

    const { isInputFocused } = useMonacoBlockInput({
        editor,
        id,
        onRunBlock,
        onBlockInputChange,
        onSelectBlock,
        onMoveBlockSelection,
    })

    // setTimeout executes the editor focus in a separate run-loop which prevents adding a newline at the start of the input
    const onEnterBlock = useCallback(() => {
        setTimeout(() => editor?.focus(), 0)
    }, [editor])
    const { onSelect } = useBlockSelection({
        id,
        blockElement: blockElement.current,
        onSelectBlock,
        isSelected,
        isInputFocused,
    })
    const { onKeyDown } = useBlockShortcuts({
        id,
        isMacPlatform,
        onMoveBlockSelection,
        onEnterBlock,
        onDeleteBlock,
        onRunBlock,
        onMoveBlock,
        onDuplicateBlock,
    })

    const modifierKeyLabel = isMacPlatform ? '⌘' : 'Ctrl'
    const mainMenuAction = useMemo(() => {
        const isLoading = searchResults && searchResults.state === 'loading'
        return {
            label: isLoading ? 'Searching...' : 'Run search',
            isDisabled: isLoading ?? false,
            icon: <PlayCircleOutlineIcon className="icon-inline" />,
            onClick: onRunBlock,
            keyboardShortcutLabel: `${modifierKeyLabel} + ↵`,
        }
    }, [onRunBlock, modifierKeyLabel, searchResults])

    const menuActions = useMemo(
        () => [
            {
                label: 'Duplicate',
                icon: <ContentDuplicateIcon className="icon-inline" />,
                onClick: onDuplicateBlock,
                keyboardShortcutLabel: `${modifierKeyLabel} + D`,
            },
            {
                label: 'Move Up',
                icon: <ArrowUpIcon className="icon-inline" />,
                onClick: id => onMoveBlock(id, 'up'),
                keyboardShortcutLabel: `${modifierKeyLabel} + ↑`,
            },
            {
                label: 'Move Down',
                icon: <ArrowDownIcon className="icon-inline" />,
                onClick: id => onMoveBlock(id, 'down'),
                keyboardShortcutLabel: `${modifierKeyLabel} + ↓`,
            },
            {
                label: 'Delete',
                icon: <DeleteIcon className="icon-inline" />,
                onClick: onDeleteBlock,
                keyboardShortcutLabel: `${modifierKeyLabel} + ⌫`,
            },
        ],
        [onDuplicateBlock, onMoveBlock, onDeleteBlock, modifierKeyLabel]
    )

    return (
        <div className={classNames('block-wrapper', blockStyles.blockWrapper)}>
            {/* eslint-disable-next-line jsx-a11y/no-static-element-interactions */}
            <div
                className={classNames(
                    blockStyles.block,
                    styles.block,
                    isSelected && !isInputFocused && blockStyles.selected,
                    isSelected && isInputFocused && blockStyles.selectedNotFocused
                )}
                onClick={onSelect}
                onKeyDown={onKeyDown}
                onFocus={onSelect}
                // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
                tabIndex={0}
                // eslint-disable-next-line jsx-a11y/aria-role
                role="notebook-block"
                aria-label="Notebook block"
                data-block-id={id}
                ref={blockElement}
            >
                {/* eslint-disable-next-line jsx-a11y/no-static-element-interactions */}
                <div
                    className={classNames(
                        blockStyles.monacoWrapper,
                        isInputFocused && blockStyles.selected,
                        styles.queryInputMonacoWrapper
                    )}
                    onKeyDown={event => event.stopPropagation()}
                >
                    <MonacoEditor
                        language={SOURCEGRAPH_SEARCH}
                        value={input}
                        height="auto"
                        isLightTheme={isLightTheme}
                        editorWillMount={() => {}}
                        onEditorCreated={setEditor}
                        options={MONACO_BLOCK_INPUT_OPTIONS}
                        border={false}
                    />
                </div>

                {searchResults && searchResults.state === 'loading' && (
                    <div className={classNames('d-flex justify-content-center py-3', styles.results)}>
                        <LoadingSpinner />
                    </div>
                )}
                {searchResults && searchResults.state !== 'loading' && (
                    <div className={styles.results}>
                        <StreamingSearchResultsList
                            location={location}
                            allExpanded={false}
                            results={searchResults}
                            isLightTheme={isLightTheme}
                            fetchHighlightedFileLineRanges={fetchHighlightedFileLineRanges}
                            telemetryService={telemetryService}
                            settingsCascade={settingsCascade}
                        />
                    </div>
                )}
            </div>
            {isSelected && <SearchNotebookBlockMenu id={id} mainAction={mainMenuAction} actions={menuActions} />}
        </div>
    )
}